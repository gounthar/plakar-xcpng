package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"integration-xcpng/internal/watchdog"
	"integration-xcpng/internal/xapi"
)

// WatchdogArg, as the first argument, makes the importer binary run as the snapshot
// watchdog instead of as a plugin (see package watchdog). A snapshot is listed by its
// UUID; while VM.snapshot has not returned, as "<vm uuid>/<label>".
const WatchdogArg = "--xcpng-snapshot-watchdog"

func startWatchdog(config map[string]string) (*watchdog.Watchdog, error) {
	return watchdog.Start("snapshot watchdog", WatchdogArg, config)
}

// RunWatchdog is the watchdog child's main. It returns the process exit status.
func RunWatchdog(stdin io.Reader) int {
	watchdog.IgnoreSignals()

	config, pending, err := watchdog.Read(stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: snapshot watchdog: %s\n", err)
		return 1
	}
	if len(pending) == 0 {
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), watchdog.CleanupTimeout)
	defer cancel()
	if err := destroyLeftovers(ctx, config, pending); err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: snapshot watchdog: %s\n", err)
		return 1
	}
	return 0
}

func destroyLeftovers(ctx context.Context, config map[string]string, uuids []string) error {
	cfg, err := xapi.ParseConfig(config)
	if err != nil {
		return err
	}
	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	p := &Importer{cfg: cfg, client: client}
	return errors.Join(p.destroyListed(ctx, uuids), client.Close(ctx))
}

// destroyListed destroys what the watchdog still lists: snapshots by UUID, and by
// label those whose VM.snapshot call never returned.
func (p *Importer) destroyListed(ctx context.Context, uuids []string) error {
	client := p.client
	var errs []error
	for _, uuid := range uuids {
		if vmUUID, label, ok := strings.Cut(uuid, "/"); ok {
			errs = append(errs, p.destroyLabelledLeftover(ctx, vmUUID, label))
			continue
		}
		var ref string
		if err := client.Call(ctx, &ref, "VM.get_by_uuid", uuid); err != nil {
			if xapi.IsCode(err, "UUID_INVALID") {
				continue // already gone
			}
			errs = append(errs, fmt.Errorf("snapshot %s: %w", uuid, err))
			continue
		}
		// Only a snapshot this connector marked as itself.
		var s snapshotSummary
		if err := client.Call(ctx, &s, "VM.get_record", ref); err != nil {
			errs = append(errs, fmt.Errorf("snapshot %s: %w", uuid, err))
			continue
		}
		if s.UUID != uuid || s.OtherConfig[snapshotMarker] != uuid {
			continue
		}
		if err := p.destroySnapshot(ctx, ref); err != nil {
			errs = append(errs, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "xcpng: destroyed snapshot %s left by an interrupted backup\n", uuid)
	}
	return errors.Join(errs...)
}

// destroyLabelledLeftover destroys the snapshot a killed plugin was waiting for.
func (p *Importer) destroyLabelledLeftover(ctx context.Context, vmUUID, label string) error {
	var vmRef string
	if err := p.client.Call(ctx, &vmRef, "VM.get_by_uuid", vmUUID); err != nil {
		if xapi.IsCode(err, "UUID_INVALID") {
			return nil // the VM is gone, and its snapshots cannot be found by label
		}
		return fmt.Errorf("snapshot %s of VM %s: %w", label, vmUUID, err)
	}
	n, err := p.destroyLabelled(ctx, vmRef, label)
	if err != nil {
		return fmt.Errorf("snapshot %s of VM %s: %w", label, vmUUID, err)
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "xcpng: destroyed snapshot %s of VM %s left by an interrupted backup\n", label, vmUUID)
	}
	return nil
}

// IsWatchdog reports whether the process was started as the snapshot watchdog.
func IsWatchdog(args []string) bool { return watchdog.Is(args, WatchdogArg) }
