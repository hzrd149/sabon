---
tags:
  - Troubleshooting
  - Operations
---

# Troubleshooting

Almost every diagnosis starts the same way — validate the config and read the
orchestrator log:

```sh
docker compose exec sabon /sabon validate   # parse targets + every label, print what resolved
docker compose logs -f sabon               # discovery, schedules, mover lifecycle
```

`validate` never touches a repository; it only reports what sabon *would* do, so
it is safe to run any time.

## No movers spawn / nothing gets backed up

A container is only a job if it carries **both** `sabon.enable: "true"` **and** a
valid `sabon.backup` spec. In **swarm mode** the labels must be on the service
(`deploy.labels`), not on the container. Check that `sabon validate` lists the app,
then look for exited movers:

```sh
docker ps -a  --filter label=sabon.mover        # standalone
docker service ls --filter label=sabon.mover    # swarm
```

Each finished mover is retained as run history (see
[`SABON_MOVER_HISTORY`](configuration.md#sabon_mover_history)); its restic output
is in `docker logs <id>` / `docker service logs <id>`.

## `403` / permission denied against the Docker API

You are almost certainly behind a socket proxy that is missing an object group.
sabon needs `POST=1` **plus**:

- standalone — `CONTAINERS`, `IMAGES`, `VOLUMES`, `EXEC`, `EVENTS`
- swarm — `SERVICES`, `TASKS`, `NODES`, `IMAGES` (point it at a **manager**)

See [socket access](deployment.md#socket) for a complete proxy example.

## `repository is already locked`

A lock can belong to an **active** backup, restore, check, or prune, including a
mover that survived a daemon restart or a separate CLI invocation. sabon leaves
repository locks intact and relies on restic's native locking: compatible
operations may overlap; incompatible operations fail and report restic's error.
Retry after the current operation finishes. The orchestrator's keyed mutex only
serialises work in that process, not across daemons or CLI invocations.

sabon does not automatically run `unlock`, even at startup. A killed mover can
therefore leave a lock requiring manual recovery. Before unlocking:

1. Pause scheduling and prevent new CLI and external restic operations against
   the repository, including on other hosts or swarm nodes.
2. Inspect mover containers/services and their logs, and verify that **all**
   repository users have stopped. Stopping the daemon alone is insufficient:
   non-cold movers may continue running after it exits.
3. With the same repository credentials, run restic's stale-lock cleanup:

```sh
restic -r <repo> unlock
```

Plain `unlock` uses lock age and, for a matching hostname, PID liveness. A PID
absent in one container says nothing about a process in another PID namespace;
hostname reuse does not establish ownership. A fresh lock from another hostname
may remain until restic considers it stale (normally 30 minutes without refresh).
Do not use `unlock --remove-all` to bypass contention. If stale cleanup leaves a
lock, keep the repository quiescent and investigate its owner before considering
manual removal. Resume scheduling only after recovery is complete.

A `prune` job locks the repo for its whole duration and blocks backups — that is
expected; see [check & prune](backups.md#checks). Native locks protect repository
operations, not the entire multi-command backup/retention sequence or application
hooks and restore destinations. Separate orchestrators can still overlap hooks,
backups, or writes to the same restore destination; coordinate those workflows
externally. Retention can fail on a lock acquired after backup completes; the
snapshot remains, and the run reports the retention error. Keep restic locking
enabled (do not pass `--no-lock` through `resticArgs`).

## `wrong password` / repository not found

A restic repository's password is fixed when it is first created. If
`RESTIC_PASSWORD` (or a per-target password) later differs, restic cannot open the
repo. Restore the original password, or start a new repository. The repo location
is `<target>/<app>` (e.g. `/mnt/backup/immich`).

## `snapshot: zfs` failed the backup

`snapshot: zfs` is strict: the source must actually live on a ZFS dataset, and the
snapshotter only runs on **standalone Docker**. It fails (rather than silently
copying live data) when the source is not on ZFS, and it is **unsupported in
swarm**. Use `snapshot: auto` (snapshot what is on ZFS, mount the rest live) or
`none`. See [Snapshots](backups.md#snapshots).

If the log shows `nsenter: failed to execute zfs: No such file or directory`, the
host's `zfs` binary is not on the snapshotter's `PATH`. The default image searches
the FHS directories (`/usr/sbin`, `/sbin`, …) plus non-FHS system paths (e.g.
`/run/current-system/sw/bin` on NixOS); for any other location, point
`SABON_SNAPSHOT_ZFS_IMAGE` at an image whose `PATH` includes it.

## Swarm: the mover never starts / image pull fails

Movers run as services scheduled onto arbitrary nodes, so the mover image must be
pullable on **every** node. It defaults to `ghcr.io/davidborzek/sabon/mover` at
the orchestrator's version; with a private mirror, set it explicitly and make
sure every node can pull it:

```sh
SABON_MOVER_IMAGE=registry.example.internal/sabon/mover:X.Y.Z
```

## Mover fails with `exec: "/usr/local/bin/sabon-mover": no such file`

`SABON_MOVER_IMAGE` points at an image without the mover binary, e.g. a mirror
or fork of the orchestrator image. Point it at a mover image instead, or unset
it to use the version-matched default.

Older setups pinned the official orchestrator image here
(`ghcr.io/davidborzek/sabon:<tag>`). sabon rewrites exactly that to
`ghcr.io/davidborzek/sabon/mover:<tag>` and logs a warning, so they keep
working; update the setting to silence it.

Also check the mover isn't stuck `Pending`: a `node.hostname` placement constraint
with no matching node (e.g. the app's node left the cluster) leaves it unscheduled.

## A local target only backs up on one node (swarm)

A local `path:` target is a host directory on whichever node the mover lands on —
it is meaningless cluster-wide. On a multi-node cluster use a **remote** target
(S3/R2/…) so every node writes to the same repository.

## Restores look empty

Sources are stored under `/data/<name>` inside the snapshot, so an `--include`
must use that prefix (`--include /data/immich-dbdump`, not `/etc`). See
[Path mapping](restore.md#path-mapping).
