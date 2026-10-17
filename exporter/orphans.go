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
// VM is fully made: the VM keeps its own name, so the name cannot carry it.
const restoreMarkerKey = "plakar_restore"

// restoreWait bounds the wait for a VDI.create xapi is still running on the SR.
var restoreWait = 10 * time.Minute

// vmCreateWait bounds the search for a VM whose VM.create never answered. xapi shows
// no operation while it creates a VM, so a VM not found yet may still appear; the
// create is one database write, so this value is a generous guess, not a measure.
var vmCreateWait = 2 * time.Minute

// restorePollDelay is the wait between two looks at a create xapi may still be running.
var restorePollDelay = 2 * time.Second

// restoreLabel names a created object so that it can be found again: the time alone
// is shared by two restores started in the same second.
func restoreLabel() string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24: it crashes the program instead
	return fmt.Sprintf("plakar-restore-%s-%s", time.Now().UTC().Format("20060102T150405Z"), hex.EncodeToString(b[:]))
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

// destroyLabelledVDI destroys the VDIs of the SR named label, once xapi has finished
// creating them. VDI.create holds a vdi_create operation on the SR until the VDI's
// record is written (message_forwarding.ml, with_sr_andor_vdi); earlier, the record
// may not exist yet. The operation is keyed by a task the caller cannot see, so any
// vdi_create on the SR is waited for.
func (p *Exporter) destroyLabelledVDI(ctx context.Context, srRef, label string) error {
	ctx, cancel := context.WithTimeout(ctx, restoreWait)
	defer cancel()
	for {
		var ops map[string]string
		if err := p.client.Call(ctx, &ops, "SR.get_current_operations", srRef); err != nil {
			return err
		}
		if !slices.Contains(slices.Collect(maps.Values(ops)), "vdi_create") {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("a VDI is still being created on the SR: %w", ctx.Err())
		case <-time.After(restorePollDelay):
		}
	}
	var refs []string
	if err := p.client.Call(ctx, &refs, "VDI.get_by_name_label", label); err != nil {
		return err
	}
	var errs []error
	for _, ref := range refs {
		var sr string
		err := p.client.Call(ctx, &sr, "VDI.get_SR", ref)
		if err == nil && sr == srRef {
			err = p.client.Call(ctx, nil, "VDI.destroy", ref)
		}
		if err != nil && !xapi.IsCode(err, "HANDLE_INVALID") { // else destroyed since the list was read
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// destroyMarkedVM destroys the VM named name whose other_config marks it with label,
// looking again until vmCreateWait in case xapi has not created it yet.
func (p *Exporter) destroyMarkedVM(ctx context.Context, name, label string) error {
	ctx, cancel := context.WithTimeout(ctx, vmCreateWait)
	defer cancel()
	for {
		found, err := p.destroyMarked(ctx, name, label)
		if err != nil || found {
			return err
		}
		select {
		case <-ctx.Done():
			warnf("VM %q: VM.create got no answer and no VM marked other_config:%s=%s appeared within %s; one that appears later was left by this restore",
				name, restoreMarkerKey, label, vmCreateWait)
			return nil
		case <-time.After(restorePollDelay):
		}
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
