package api

import (
	"fmt"
	"strings"
)

func (t Target) validateDestinationGuard() error {
	if t.DestinationMarker != "" {
		if t.Backend() != BackendLocal {
			return fmt.Errorf("target %q: destinationMarker requires a local path backend", t.Name)
		}
		if strings.TrimSpace(t.DestinationMarker) != t.DestinationMarker {
			return fmt.Errorf("target %q: destinationMarker must not have surrounding whitespace", t.Name)
		}
	}
	if t.DestinationMarker == "" && !t.RequireExistingRepository {
		return nil
	}
	for key := range t.Env {
		if strings.HasPrefix(key, "RESTIC_REPOSITORY") || key == "RESTIC_FROM_REPOSITORY" {
			return fmt.Errorf("target %q: destination guard forbids repository overrides in env", t.Name)
		}
	}
	for _, arg := range t.ResticArgs {
		if strings.HasPrefix(arg, "--repo") || strings.HasPrefix(arg, "-r") && !strings.HasPrefix(arg, "--") {
			return fmt.Errorf("target %q: destination guard forbids repository overrides in resticArgs", t.Name)
		}
	}
	return nil
}
