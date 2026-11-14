package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// Lock files are permanent: unlinking a locked inode would allow a second owner.
func coldLock(ctx context.Context, path string, wait bool) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !wait || (!errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN)) {
			_ = f.Close()
			return nil, err
		}
		t := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			_ = f.Close()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

func coldKey(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

func syncColdDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func writeColdRecord(dir string, r *coldRecord) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".intent-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, coldKey(r.Container)+".json")); err != nil {
		return err
	}
	return syncColdDir(dir)
}

func readColdRecords(dir string) ([]coldRecord, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var records []coldRecord
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		} // another owner completed
		if err != nil {
			return nil, err
		}
		var r coldRecord
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil {
			return nil, err
		}
		for _, key := range []string{"version", "operation", "container", "was_running", "sources", "phase"} {
			if value, ok := fields[key]; !ok || string(value) == "null" && key != "sources" {
				return nil, fmt.Errorf("cold journal missing %s", key)
			}
		}
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("invalid cold journal %s: %w", entry.Name(), err)
		}
		if r.Version != 1 || r.Container == "" || r.Operation == "" || entry.Name() != coldKey(r.Container)+".json" {
			return nil, fmt.Errorf("invalid or unsupported cold journal %s", entry.Name())
		}
		switch r.Phase {
		case "stop-intent", "stopped", "mover-intent", "resume-intent":
		default:
			return nil, fmt.Errorf("unknown cold phase %q", r.Phase)
		}
		if (r.Phase == "mover-intent" || r.MoverName != "") && r.MoverName != "sabon-cold-"+r.Operation {
			return nil, fmt.Errorf("invalid cold mover identity")
		}
		records = append(records, r)
	}
	return records, nil
}

func removeColdRecord(dir string, r *coldRecord) error {
	err := os.Remove(filepath.Join(dir, coldKey(r.Container)+".json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncColdDir(dir)
}
