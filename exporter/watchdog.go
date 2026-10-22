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
//
// A VDI or VM xapi says is busy is put aside and retried, with the others put aside,
// every restorePollDelay for up to restoreWait. The watchdog starts the moment the
// plugin dies, and xapi still holds a VDI for an import whose connection was just cut:
// on XCP-ng 8.3 the first VDI.destroy got VDI_IN_USE. The hold has been seen to last
// about two minutes once and under two seconds once, so how long it lasts is not known.
// Each key gets its first try before any wait, so one held long
// cannot use up the watchdog's time while the others wait their turn.
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
	var busy []string
	last := map[string]error{}
	try := func(key string) {
		// Once the watchdog's time is up, name what is left instead of trying it.
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
			return
		}
		switch err := p.destroyKey(ctx, key); {
		case heldByXapi(err):
			busy = append(busy, key)
			last[key] = err
		case errors.Is(err, errGone):
		case err != nil:
			// A try cut short by the watchdog's time says nothing of why the key was
			// put aside, so the busy answer before it is kept.
			errs = append(errs, fmt.Errorf("%s: %w", key, errors.Join(last[key], err)))
		default:
			fmt.Fprintf(os.Stderr, "xcpng: cleaned up %s left by an interrupted restore\n", key)
		}
	}
	for _, key := range keys {
		try(key)
	}

	deadline := time.Now().Add(restoreWait)
	for len(busy) > 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		select {
		case <-ctx.Done():
			continue
		case <-time.After(restorePollDelay):
		}
		retry := busy
		busy = nil
		for _, key := range retry {
			try(key)
		}
	}
	for _, key := range busy {
		errs = append(errs, fmt.Errorf("%s: %w", key, errors.Join(last[key], ctx.Err())))
	}
	return errors.Join(errs...)
}

// errGone is what destroyKey returns for a key with nothing left to destroy.
var errGone = errors.New("nothing to destroy")

// destroyKey destroys what one watchdog key names.
func (p *Exporter) destroyKey(ctx context.Context, key string) error {
	kind, rest, _ := strings.Cut(key, "/")
	switch kind {
	case "vm", "vdi":
		method := "VDI.destroy"
		if kind == "vm" {
			method = "VM.destroy"
		}
		// destroyRef retries past ctx, so it is given only the watchdog's time left.
		bound := cleanupRetry
		if d, ok := ctx.Deadline(); ok {
			bound = min(bound, time.Until(d))
		}
		err := p.destroyRef(ctx, bound, method, rest)
		if xapi.IsCode(err, "HANDLE_INVALID") {
			return errGone
		}
		return err
	case "vdi-label":
		srRef, label, _ := strings.Cut(rest, "/")
		return p.destroyLabelledVDI(ctx, srRef, label)
	case "vm-label":
		label, quoted, _ := strings.Cut(rest, "/")
		name, err := strconv.Unquote(quoted)
		if err != nil {
			return err
		}
		return p.destroyMarkedVM(ctx, name, label)
	}
	return errGone
}

// IsWatchdog reports whether the process was started as the restore watchdog.
func IsWatchdog(args []string) bool { return watchdog.Is(args, WatchdogArg) }
