package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"integration-xcpng/internal/watchdog"
	"integration-xcpng/internal/xapi"
)

// WatchdogArg, as the first argument, makes the exporter binary run as the restore
// watchdog instead of as a plugin (see package watchdog).
//
// Every VDI and VM the restore makes is listed from before its create is sent until it
// is kept as part of a restored VM or destroyed, so a plugin killed in between, which is
// what an interrupted plakar does, leaves nothing behind. A key says what it names:
// "vdi/<ref>" and "vm/<ref>" once the create has answered, "vdi-label/<SR ref>/<label>"
// and "vm-label/<label>/<VM name, Go-quoted>" while it has not. A reference never
// holds a slash, nor does a label, so the VM name, which may, comes last.
const WatchdogArg = "--xcpng-restore-watchdog"

func vdiKey(ref string) string               { return "vdi/" + ref }
func vmKey(ref string) string                { return "vm/" + ref }
func vdiLabelKey(srRef, label string) string { return "vdi-label/" + srRef + "/" + label }

// vmLabelKey quotes the VM name, which comes from the backup's vm.json, so that a
// name holding a line break cannot write protocol lines of its own.
func vmLabelKey(name, label string) string { return "vm-label/" + label + "/" + strconv.Quote(name) }
func isVMKey(key string) bool              { return strings.HasPrefix(key, "vm") }
func destroyedKey(method, ref string) string {
	if method == "VM.destroy" {
		return vmKey(ref)
	}
	return vdiKey(ref)
}

// watch lists key with the watchdog, starting it on first use. Without a watchdog the
// restore still runs: only the in-process cleanup then covers a failure.
func (p *Exporter) watch(key string) {
	if p.watchdog == nil {
		wd, err := watchdog.Start("restore watchdog", WatchdogArg, p.config)
		if err != nil {
			warnf("%s: a killed restore will leave %s behind", err, key)
			return
		}
		p.watchdog = wd
	}
	if err := p.watchdog.Add(key); err != nil {
		warnf("%s: a killed restore will leave %s behind", err, key)
	}
}

func (p *Exporter) unwatch(key string) {
	if p.watchdog != nil {
		_ = p.watchdog.Remove(key)
	}
}

// keep unlists a VM the restore keeps, and its disks: from here a killed plugin must
// not take them with it.
func (p *Exporter) keep(vmRef string, vm pendingVM, restored map[string]restoredDisk) {
	p.unwatch(vmKey(vmRef))
	for _, d := range vm.meta.Disks {
		if disk, ok := restored[vm.dir+"/"+d.VDIUUID]; ok {
			p.unwatch(vdiKey(disk.ref))
		}
	}
}

// RunWatchdog is the watchdog child's main. It returns the process exit status.
func RunWatchdog(stdin io.Reader) int {
	watchdog.IgnoreSignals()

	config, pending, err := watchdog.Read(stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: restore watchdog: %s\n", err)
		return 1
	}
	if len(pending) == 0 {
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), watchdog.CleanupTimeout)
	defer cancel()
	if err := destroyLeftovers(ctx, config, pending); err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: restore watchdog: %s\n", err)
		return 1
	}
	return 0
}

func destroyLeftovers(ctx context.Context, config map[string]string, keys []string) error {
	cfg, err := xapi.ParseConfig(config)
	if err != nil {
		return err
	}
	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	p := &Exporter{cfg: cfg, client: client}
	return errors.Join(p.destroyListed(ctx, keys), client.Close(ctx))
}

// destroyListed destroys what the watchdog still lists, VMs first: VM.destroy leaves
// the VDIs, which then have no VBD left to hold them.
func (p *Exporter) destroyListed(ctx context.Context, keys []string) error {
	keys = slices.Clone(keys)
	slices.SortStableFunc(keys, func(a, b string) int {
		switch {
		case isVMKey(a) == isVMKey(b):
			return 0
		case isVMKey(a):
			return -1
		}
		return 1
	})
	var errs []error
	for _, key := range keys {
		// The cleanup calls retry past ctx (destroyRef runs them without its
		// cancellation), so once the watchdog's time is up, name what is left instead.
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			continue
		}
		kind, rest, _ := strings.Cut(key, "/")
		var err error
		switch kind {
		case "vm", "vdi":
			method := "VDI.destroy"
			if kind == "vm" {
				method = "VM.destroy"
			}
			err = p.destroyWhenFree(ctx, method, rest)
			if xapi.IsCode(err, "HANDLE_INVALID") {
				continue // already gone
			}
		case "vdi-label":
			srRef, label, _ := strings.Cut(rest, "/")
			err = p.destroyLabelledVDI(ctx, srRef, label)
		case "vm-label":
			label, quoted, _ := strings.Cut(rest, "/")
			var name string
			if name, err = strconv.Unquote(quoted); err == nil {
				err = p.destroyMarkedVM(ctx, name, label)
			}
		default:
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			continue
		}
		fmt.Fprintf(os.Stderr, "xcpng: cleaned up %s left by an interrupted restore\n", key)
	}
	return errors.Join(errs...)
}

// destroyWhenFree destroys a VDI or a VM, waiting up to restoreWait while xapi says it
// is busy. The watchdog starts the moment the plugin dies, and xapi still holds a VDI
// for an import whose connection was just cut: on XCP-ng 8.3 the first VDI.destroy got
// VDI_IN_USE, and the VDI was free to destroy two minutes later.
func (p *Exporter) destroyWhenFree(ctx context.Context, method, ref string) error {
	deadline := time.Now().Add(restoreWait)
	for {
		err := p.destroyRef(ctx, method, ref)
		busy := xapi.IsCode(err, "VDI_IN_USE") || xapi.IsCode(err, "OTHER_OPERATION_IN_PROGRESS")
		if !busy || !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(restorePollDelay):
		}
	}
}

// IsWatchdog reports whether the process was started as the restore watchdog.
func IsWatchdog(args []string) bool { return watchdog.Is(args, WatchdogArg) }
