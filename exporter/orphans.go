package exporter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"time"

	"integration-xcpng/internal/xapi"
)

// A VDI.create or VM.create that gets no answer (connection lost, interrupted, the
// call bound reached) may still have been carried out by xapi, and then nobody holds
// the new object's reference. Each create therefore carries a label unique to this
// restore, so the object can be found again and destroyed.

// restoreMarkerKey is the other_config key holding a restored VM's label until the
// VM is kept: the VM keeps its own name, so the name cannot carry it.
const restoreMarkerKey = "plakar_restore"

// restoreWait bounds the wait for a VDI.create xapi is still running on the SR.
var restoreWait = 10 * time.Minute

// vmCreateWait bounds the search for a VM whose VM.create never answered. xapi shows
// no operation while it creates a VM, so a VM not found yet may still appear; the
// create is one database write, so this value is a generous guess, not a measure.
var vmCreateWait = 2 * time.Minute

// cleanupRetry is how long a cleanup call is retried while xapi does not answer: a
// toolstack restart refuses connections for some seconds, then forgets every session.
var cleanupRetry = 3 * time.Minute

// attemptTimeout bounds each cleanup call. The client bounds a call only when its
// context has no deadline, and the cleanups run under one, so a call left hanging on
// a dead connection would otherwise use up the whole wait.
var attemptTimeout = time.Minute

// restorePollDelay is the wait between two looks at a create xapi may still be running.
var restorePollDelay = 2 * time.Second

// randomHex returns n random bytes in hex.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never fails since Go 1.24: it crashes the program instead
	return hex.EncodeToString(b)
}

// restoreID names this restore, the prefix of every label it gives: a user told the
// watchdog is gone can search the pool for it. The time alone is shared by two
// restores started in the same second.
func (p *Exporter) restoreID() string {
	if p.id == "" {
		p.id = fmt.Sprintf("plakar-restore-%s-%s", time.Now().UTC().Format("20060102T150405Z"), randomHex(4))
	}
	return p.id
}

// restoreLabel names a created object so that it can be found again.
func (p *Exporter) restoreLabel() string {
	return p.restoreID() + "-" + randomHex(4)
}

// neverSent reports whether a call failed before its request left: the connection to
// xapi could not be opened, so xapi cannot have run it.
func neverSent(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

// mayHaveRun reports whether xapi may have carried out a call that failed: it did not
// answer with an error of its own, and the request may have reached it.
func mayHaveRun(err error) bool {
	var xe *xapi.Error
	return !errors.As(err, &xe) && !neverSent(err)
}

// retry runs fn until it succeeds, xapi answers it with an error of its own, or
// bound runs out. An unanswered call is tried again, and a lost session is renewed
// first. attempt counts from 0, so fn can tell that an earlier try may have done its
// work and lost the answer.
func (p *Exporter) retry(ctx context.Context, bound time.Duration, fn func(ctx context.Context, attempt int) error) error {
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	delay := restorePollDelay
	for attempt := 0; ; attempt++ {
		actx, acancel := context.WithTimeout(ctx, attemptTimeout)
		err := fn(actx, attempt)
		acancel()
		if err == nil {
			return nil
		}
		again := xapi.IsUnavailable(err)
		if xapi.IsCode(err, "SESSION_INVALID") || xapi.IsCode(err, "HOST_IS_SLAVE") {
			// xapi restarted and forgot the session, or the pool has a new master.
			lerr := p.client.Relogin(ctx)
			again = lerr == nil || xapi.IsUnavailable(lerr)
			err = errors.Join(err, lerr)
		}
		if !again {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(2*delay, 10*time.Second)
	}
}

// destroyRef destroys a VDI or a VM, retrying for up to bound while xapi does not
// answer, and unlists it from the watchdog. A handle gone on a retry was destroyed by
// an earlier try whose answer was lost. The retries outlive ctx, which an interrupt
// cancels before the cleanup runs, so a caller with a deadline of its own passes the
// time left as bound.
func (p *Exporter) destroyRef(ctx context.Context, bound time.Duration, method, ref string) error {
	err := p.retry(context.WithoutCancel(ctx), bound, func(ctx context.Context, attempt int) error {
		err := p.client.Call(ctx, nil, method, ref)
		if attempt > 0 && xapi.IsCode(err, "HANDLE_INVALID") {
			return nil
		}
		return err
	})
	if err == nil {
		p.unwatch(destroyedKey(method, ref))
	}
	return err
}

// heldByXapi reports whether xapi refused a destroy because it still holds the object: an
// import whose connection was just cut keeps its VDI for a while (see destroyListed).
func heldByXapi(err error) bool {
	return xapi.IsCode(err, "VDI_IN_USE") || xapi.IsCode(err, "OTHER_OPERATION_IN_PROGRESS")
}

// heldWait bounds how long the restore's own cleanup waits for xapi to let go of the
// VDIs it holds, all of them together: the watchdog destroys any still held when the
// plugin exits, so the failing record need not wait for it.
var heldWait = 3 * time.Minute

// destroyVDIs is destroyRef for the restore's own cleanup of VDIs. Each is tried once,
// then those xapi still holds are tried again together every restorePollDelay until
// heldWait runs out, so several held disks cost one wait, not one each. Left to the
// watchdog instead, a VDI would be reported as left by an interrupted restore. Once ctx
// is done the waiting stops, the held VDIs stay listed, and the watchdog destroys them
// when the plugin exits: an interrupted restore must not hold up its exit for them.
// It returns the error of each VDI not destroyed.
func (p *Exporter) destroyVDIs(ctx context.Context, refs []string) map[string]error {
	errs := map[string]error{}
	try := func(refs []string) (held []string) {
		for _, ref := range refs {
			err := p.destroyRef(ctx, cleanupRetry, "VDI.destroy", ref)
			if err == nil {
				delete(errs, ref)
				continue
			}
			errs[ref] = err
			if heldByXapi(err) {
				held = append(held, ref)
			}
		}
		return held
	}
	deadline := time.Now().Add(heldWait)
	for held := try(refs); len(held) > 0 && time.Now().Before(deadline); held = try(held) {
		select {
		case <-ctx.Done():
			return errs
		case <-time.After(restorePollDelay):
		}
	}
	return errs
}

// destroyLabelledVDI destroys the VDIs of the SR named label, once xapi has finished
// creating them. VDI.create holds a vdi_create operation on the SR until the VDI's
// record is written (message_forwarding.ml, with_sr_andor_vdi); earlier, the record
// may not exist yet. The operation is keyed by a task the caller cannot see, so any
// vdi_create on the SR is waited for. When the wait runs out, which another client's
// creates can cause on a busy SR, the VDI is looked for all the same: it is most
// likely made by then.
func (p *Exporter) destroyLabelledVDI(ctx context.Context, srRef, label string) error {
	var waitErr error
	deadline := time.Now().Add(restoreWait)
	for {
		var busy bool
		err := p.retry(ctx, cleanupRetry, func(ctx context.Context, _ int) error {
			var ops map[string]string
			err := p.client.Call(ctx, &ops, "SR.get_current_operations", srRef)
			busy = slices.Contains(slices.Collect(maps.Values(ops)), "vdi_create")
			return err
		})
		if err != nil {
			waitErr = err
			break
		}
		if !busy {
			break
		}
		if !time.Now().Before(deadline) {
			waitErr = fmt.Errorf("a VDI was still being created on the SR after %s", restoreWait)
			break
		}
		time.Sleep(restorePollDelay)
	}
	n := 0
	err := p.retry(ctx, cleanupRetry, func(ctx context.Context, _ int) error {
		var refs []string
		if err := p.client.Call(ctx, &refs, "VDI.get_by_name_label", label); err != nil {
			return err
		}
		var errs []error
		for _, ref := range refs {
			var sr string
			err := p.client.Call(ctx, &sr, "VDI.get_SR", ref)
			if err == nil && sr == srRef {
				if err = p.client.Call(ctx, nil, "VDI.destroy", ref); err == nil {
					n++
				}
			}
			if err != nil && !xapi.IsCode(err, "HANDLE_INVALID") { // else destroyed since the list was read
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
	if err != nil {
		return errors.Join(waitErr, err)
	}
	if n == 0 {
		return waitErr // nil when the wait ended: xapi never made the VDI
	}
	return nil
}

// destroyMarkedVM destroys the VM named name whose other_config marks it with label,
// looking again until vmCreateWait in case xapi has not created it yet.
func (p *Exporter) destroyMarkedVM(ctx context.Context, name, label string) error {
	deadline := time.Now().Add(vmCreateWait)
	for {
		var found bool
		err := p.retry(ctx, cleanupRetry, func(ctx context.Context, _ int) error {
			f, err := p.destroyMarked(ctx, name, label)
			found = found || f // a retry finds nothing after a destroy whose answer was lost
			return err
		})
		if err != nil || found {
			return err
		}
		if !time.Now().Before(deadline) {
			warnf("VM %q: VM.create got no answer and no VM marked other_config:%s=%s appeared within %s; one that appears later was left by this restore",
				name, restoreMarkerKey, label, vmCreateWait)
			return nil
		}
		time.Sleep(restorePollDelay)
	}
}

func (p *Exporter) destroyMarked(ctx context.Context, name, label string) (bool, error) {
	var refs []string
	if err := p.client.Call(ctx, &refs, "VM.get_by_name_label", name); err != nil {
		return false, err
	}
	found := false
	var errs []error
	for _, ref := range refs {
		var oc map[string]string
		err := p.client.Call(ctx, &oc, "VM.get_other_config", ref)
		if err == nil && oc[restoreMarkerKey] == label {
			found = true
			err = p.client.Call(ctx, nil, "VM.destroy", ref)
		}
		if err != nil && !xapi.IsCode(err, "HANDLE_INVALID") {
			errs = append(errs, err)
		}
	}
	return found, errors.Join(errs...)
}
