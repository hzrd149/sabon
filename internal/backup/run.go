package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/mover"
)

// RunTarget returns snapshot statistics independently of the overall lifecycle
// error. A created snapshot does not imply successful application recovery.
func (o *Orchestrator) RunTarget(ctx context.Context, job discovery.Job, target api.Target) (res mover.Result, retErr error) {
	releaseApp, err := o.lifecycles.lock(ctx, lifecycleKey(job.App, job.Container))
	if err != nil {
		return mover.Result{}, err
	}
	defer releaseApp()
	// Per-target source view: drop volumes/paths this target excludes.
	job.Sources = o.sourcesForTarget(job, target)
	// Serialise runs against the same repository (target+app).
	unlock := o.locks.lock(target.Name + "|" + job.App)
	defer unlock()

	// Cap global concurrency (after the per-repo lock, so a run blocked on its
	// repo doesn't hold a slot).
	if err := o.acquire(ctx); err != nil {
		return mover.Result{}, err
	}
	defer o.release()

	// Apply the backup deadline (per-app label override, else the global
	// default). Waiting for the lock/slot above is not counted against it.
	timeout := o.cfg.BackupTimeout
	if d, err := job.Spec.BackupTimeout(); err != nil {
		return mover.Result{}, fmt.Errorf("invalid timeout: %w", err)
	} else if d > 0 {
		timeout = d
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	env, err := o.resticEnv(target, job.App)
	if err != nil {
		return mover.Result{}, err
	}
	if err := o.ensureCache(ctx); err != nil {
		return mover.Result{}, err
	}

	log := o.log.With("app", job.App, "target", target.Name)
	if err := o.requireNoReaders(ctx, job.App, job.Container); err != nil {
		return mover.Result{}, err
	}

	// Arm recovery before invoking a pre-hook or stop: either can change state
	// and then fail. Post-hooks must tolerate incomplete preparation.
	stopAttempted, keptMover := false, false
	var snapCleanup func(context.Context)
	defer func() {
		if keptMover {
			retErr = errors.Join(retErr, fmt.Errorf("recovery deferred: mover may still be reading sources; snapshots and hook artifacts retained; verify mover exit and recover the app manually"))
			return
		}
		if snapCleanup != nil {
			cleanupCtx, cancel := cleanupContext(ctx)
			snapCleanup(cleanupCtx)
			cancel()
		}
		if stopAttempted {
			cleanupCtx, cancel := cleanupContext(ctx)
			if err := o.quiesce.Start(cleanupCtx, job.Container); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("restart after backup: %w", err))
			}
			cancel()
		}
		// Restart first: exec-mode cleanup needs a running application.
		retErr = errors.Join(retErr, o.runPostHooks(ctx, job, log))
	}()

	// Pre-hooks (e.g. pg_dump), then optional cold-backup stop.
	if err := o.runHooks(ctx, job, job.Spec.PreHooks, "pre", log); err != nil {
		return mover.Result{}, err
	}
	if job.Spec.Stop {
		stopAttempted = true
		log.Info("stopping container for cold backup", "container", job.Container)
		if err := o.quiesce.Stop(ctx, job.Container); err != nil {
			return mover.Result{}, fmt.Errorf("stop for backup: %w", err)
		}
	}

	// Snapshot sources after pre-hooks and any cold stop, so a snapshot captures
	// dumped/quiesced data. "none" yields live source mounts.
	srcMounts, cleanup, err := o.sourceMountsFor(ctx, job)
	snapCleanup = cleanup
	if err != nil {
		return mover.Result{}, err
	}
	cacheMs, binds := o.cacheAndRepo(target, job.App)
	mounts := append(srcMounts, cacheMs...)

	req := mover.Request{
		Name:           moverName(job.App, target.Name),
		Image:          o.image,
		User:           job.Spec.MoverUser(o.cfg.MoverUser),
		Groups:         job.Spec.MoverGroups(o.cfg.MoverGroups),
		Network:        o.cfg.MoverNetwork,
		Spec:           o.moverSpec(job, target),
		Env:            env,
		Mounts:         mounts,
		Binds:          binds,
		KeepOnShutdown: !job.Spec.Stop && len(job.Spec.PreHooks) == 0 && len(job.Spec.PostHooks) == 0,
		Labels:         o.moverLabels(target, job.App, "backup"),
		Retain:         true,
		Node:           job.Node,
	}
	log.Debug("spawning mover", "name", req.Name, "image", req.Image, "user", req.User, "sources", len(srcMounts))
	req.Labels[mover.LabelSourceContainer] = job.Container
	res, kept, runErr := o.eng.RunMover(ctx, req)
	keptMover = kept

	if runErr != nil && !kept {
		// Runtime removal can fail after cancellation. Confirm exit before any
		// cleanup that could invalidate data still mounted by that mover.
		checkCtx, cancel := cleanupContext(ctx)
		if err := o.requireNoReaders(checkCtx, job.App, job.Container); err != nil {
			keptMover = true
			runErr = errors.Join(runErr, err)
		}
		cancel()
	}
	return res, runErr
}

// RunSnapshots lists the snapshots in an app's repository for a target, writing
// restic's output to out.
func (o *Orchestrator) RunSnapshots(ctx context.Context, app string, target api.Target, out io.Writer) error {
	unlock := o.locks.lock(target.Name + "|" + app)
	defer unlock()

	env, err := o.resticEnv(target, app)
	if err != nil {
		return err
	}
	if err := o.ensureCache(ctx); err != nil {
		return err
	}
	mounts, binds := o.cacheAndRepo(target, app)
	_, _, err = o.eng.RunMover(ctx, mover.Request{
		Name:    moverName(app, target.Name),
		Image:   o.image,
		User:    o.cfg.MoverUser,
		Groups:  o.cfg.MoverGroups,
		Network: o.cfg.MoverNetwork,
		Spec:    mover.Spec{App: app, Action: mover.ActionSnapshots, ExtraArgs: target.ResticArgs, DestinationMarker: target.DestinationMarker, RequireExistingRepository: target.RequireExistingRepository},
		Env:     env,
		Mounts:  mounts,
		Binds:   binds,
		Stdout:  out,
		Labels:  o.customLabels(target),
	})
	return err
}

// RunCheck runs `restic check` (repository integrity) for an app's repository
// in a target. When out is non-nil the restic output is streamed there;
// otherwise it is captured and surfaced only on failure (scheduled runs).
func (o *Orchestrator) RunCheck(ctx context.Context, app string, target api.Target, out io.Writer) error {
	unlock := o.locks.lock(target.Name + "|" + app)
	defer unlock()
	if err := o.acquire(ctx); err != nil {
		return err
	}
	defer o.release()

	env, err := o.resticEnv(target, app)
	if err != nil {
		return err
	}
	if err := o.ensureCache(ctx); err != nil {
		return err
	}
	mounts, binds := o.cacheAndRepo(target, app)
	_, _, err = o.eng.RunMover(ctx, mover.Request{
		Name:    moverName(app, target.Name),
		Image:   o.image,
		User:    o.cfg.MoverUser,
		Groups:  o.cfg.MoverGroups,
		Network: o.cfg.MoverNetwork,
		Spec:    mover.Spec{App: app, Action: mover.ActionCheck, ExtraArgs: target.ResticArgs, DestinationMarker: target.DestinationMarker, RequireExistingRepository: target.RequireExistingRepository},
		Env:     env,
		Mounts:  mounts,
		Binds:   binds,
		Stdout:  out,
		Labels:  o.moverLabels(target, app, "check"),
		Retain:  true,
	})
	return err
}

// RunPrune runs `restic prune` (repack / reclaim space) for an app's repository
// in a target. Same streaming/capture semantics as RunCheck.
func (o *Orchestrator) RunPrune(ctx context.Context, app string, target api.Target, out io.Writer) error {
	unlock := o.locks.lock(target.Name + "|" + app)
	defer unlock()
	if err := o.acquire(ctx); err != nil {
		return err
	}
	defer o.release()

	env, err := o.resticEnv(target, app)
	if err != nil {
		return err
	}
	if err := o.ensureCache(ctx); err != nil {
		return err
	}
	mounts, binds := o.cacheAndRepo(target, app)
	_, _, err = o.eng.RunMover(ctx, mover.Request{
		Name:    moverName(app, target.Name),
		Image:   o.image,
		User:    o.cfg.MoverUser,
		Groups:  o.cfg.MoverGroups,
		Network: o.cfg.MoverNetwork,
		Spec:    mover.Spec{App: app, Action: mover.ActionPrune, ExtraArgs: target.ResticArgs, DestinationMarker: target.DestinationMarker, RequireExistingRepository: target.RequireExistingRepository},
		Env:     env,
		Mounts:  mounts,
		Binds:   binds,
		Stdout:  out,
		Labels:  o.moverLabels(target, app, "prune"),
		Retain:  true,
	})
	return err
}

// RestoreOptions configure a restore run.
type RestoreOptions struct {
	Snapshot string   // snapshot ID; default "latest"
	Into     string   // host dir for a staging restore; empty => in-place
	Stop     bool     // stop the app during an in-place restore
	Include  []string // limit the restore to matching paths
}

// RunRestore restores a snapshot. With opts.Into set it restores into a host
// staging directory (safe; the app need not exist). Otherwise it restores
// in-place into the app's live volumes (job required), optionally stopping the
// app first. restic output is written to out.
func (o *Orchestrator) RunRestore(ctx context.Context, app string, job *discovery.Job, target api.Target, opts RestoreOptions, out io.Writer) (retErr error) {
	if opts.Into == "" {
		releaseApp, err := o.lifecycles.lock(ctx, lifecycleKey(app, sourceContainer(job)))
		if err != nil {
			return err
		}
		defer releaseApp()
		if err := o.requireNoReaders(ctx, app, sourceContainer(job)); err != nil {
			return err
		}
	}
	unlock := o.locks.lock(target.Name + "|" + app)
	defer unlock()

	env, err := o.resticEnv(target, app)
	if err != nil {
		return err
	}
	if err := o.ensureCache(ctx); err != nil {
		return err
	}
	mounts, binds := o.cacheAndRepo(target, app)
	spec := mover.Spec{App: app, Action: mover.ActionRestore, SnapshotID: opts.Snapshot, Include: opts.Include, ExtraArgs: target.ResticArgs, DestinationMarker: target.DestinationMarker, RequireExistingRepository: target.RequireExistingRepository}

	if opts.Into != "" {
		binds = append(binds, opts.Into+":"+restoreMount)
		spec.RestoreTarget = restoreMount
	} else {
		if job == nil {
			return fmt.Errorf("in-place restore needs the app's live container; use --into <dir> for staging")
		}
		mounts = append(mounts, o.sourceMounts(*job, false)...)
		spec.RestoreTarget = "/"
		if opts.Stop {
			log := o.log.With("app", app, "target", target.Name)
			log.Info("stopping container for in-place restore", "container", job.Container)
			defer func() {
				checkCtx, checkCancel := cleanupContext(ctx)
				err := o.requireNoReaders(checkCtx, app, sourceContainer(job))
				checkCancel()
				if err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("restart after restore deferred: %w", err))
					return
				}
				cleanupCtx, cancel := cleanupContext(ctx)
				defer cancel()
				if err := o.quiesce.Start(cleanupCtx, job.Container); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("restart after restore: %w", err))
				}
			}()
			if err := o.quiesce.Stop(ctx, job.Container); err != nil {
				return fmt.Errorf("stop for restore: %w", err)
			}
		}
	}

	// The per-app user applies to in-place restore (job present), where files
	// are written into the app's live volumes; staging restore uses the default.
	user := o.cfg.MoverUser
	groups := o.cfg.MoverGroups
	var node string
	if job != nil {
		user = job.Spec.MoverUser(o.cfg.MoverUser)
		groups = job.Spec.MoverGroups(o.cfg.MoverGroups)
		node = job.Node
	}

	labels := o.moverLabels(target, app, "restore")
	if opts.Into == "" {
		labels[mover.LabelSourceContainer] = sourceContainer(job)
	}
	_, _, err = o.eng.RunMover(ctx, mover.Request{
		Name:    moverName(app, target.Name),
		Image:   o.image,
		User:    user,
		Groups:  groups,
		Network: o.cfg.MoverNetwork,
		Spec:    spec,
		Env:     env,
		Mounts:  mounts,
		Binds:   binds,
		Stdout:  out,
		Labels:  labels,
		Retain:  true,
		Node:    node,
	})
	return err
}

// moverLabels tags a mover with the run-history labels the API reads back, on
// top of the operator-configured custom labels (global SABON_MOVER_LABELS plus
// the target's own moverLabels). sabon's own labels always win over custom ones.
func (o *Orchestrator) moverLabels(target api.Target, app, action string) map[string]string {
	l := o.customLabels(target)
	l[mover.LabelApp] = app
	l[mover.LabelTarget] = target.Name
	l[mover.LabelAction] = action
	return l
}

// customLabels returns a fresh map of the operator-configured mover labels:
// the global SABON_MOVER_LABELS with the target's own moverLabels merged on top
// (target wins). Safe for a caller to extend per run.
func (o *Orchestrator) customLabels(target api.Target) map[string]string {
	l := make(map[string]string, len(o.cfg.MoverLabels)+len(target.MoverLabels))
	for k, v := range o.cfg.MoverLabels {
		l[k] = v
	}
	for k, v := range target.MoverLabels {
		l[k] = v
	}
	return l
}
