package main

import (
	"context"
	"fmt"
	"os"

	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/urfave/cli/v3"
)

func runRecover(ctx context.Context, _ *cli.Command) error {
	// Recovery must work even when targets or credentials have been removed.
	cfg := &config.Config{Runtime: os.Getenv("SABON_RUNTIME"), StateDir: os.Getenv("SABON_STATE_DIR"), Instance: os.Getenv("SABON_INSTANCE"), LogLevel: os.Getenv("SABON_LOG_LEVEL"), Snapshot: "none"}
	logger := newLogger(cfg.LogLevel)
	cli, err := dockerClient()
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	rt, err := newRuntime(ctx, cli, cfg, logger)
	if err != nil {
		return err
	}
	if rt.mode != "standalone" {
		return fmt.Errorf("recover command currently supports standalone Docker only")
	}
	if cfg.StateDir == "" {
		return fmt.Errorf("recover requires SABON_STATE_DIR for standalone Docker")
	}
	q, ok := rt.quiesce.(engine.Recoverer)
	if !ok {
		return fmt.Errorf("runtime does not support cold recovery")
	}
	n, err := q.RecoverQuiesced(ctx)
	logger.Info("cold recovery completed", "recovered", n)
	return err
}
