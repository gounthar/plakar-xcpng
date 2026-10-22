package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"

	"integration-xcpng/internal/watchdog"
	"integration-xcpng/internal/xapi"
)

func init() {
	if err := exporter.Register(xapi.Protocol, 0, NewExporter); err != nil {
		panic(err)
	}
}

// maxMetaSize bounds the vm.json read into memory.
const maxMetaSize = 16 << 20

type options struct {
	srUUID         string
	nameSuffix     string
	startOnRestore bool
	keepMAC        bool
	keepTags       bool
	keepSBUpdate   bool
	allowShort     bool
}

type Exporter struct {
	cfg    *xapi.Config
	client *xapi.Client
	opts   options
	// config is kept for the restore watchdog, started at the first create.
	config   map[string]string
	watchdog *watchdog.Watchdog
}

func NewExporter(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (exporter.Exporter, error) {
	cfg, err := xapi.ParseConfig(config)
	if err != nil {
		return nil, err
	}

	o := options{
		srUUID:     strings.TrimSpace(config["sr_uuid"]),
		nameSuffix: " (restored)",
	}
	if v, ok := config["name_suffix"]; ok {
		o.nameSuffix = v
	}
	if o.startOnRestore, err = xapi.ParseBool(config["start_on_restore"]); err != nil {
		return nil, fmt.Errorf("start_on_restore: %w", err)
	}

	if o.keepMAC, err = xapi.ParseBool(config["keep_mac"]); err != nil {
		return nil, fmt.Errorf("keep_mac: %w", err)
	}
	if o.keepTags, err = xapi.ParseBool(config["keep_tags"]); err != nil {
		return nil, fmt.Errorf("keep_tags: %w", err)
	}
	if o.keepSBUpdate, err = xapi.ParseBool(config["keep_secureboot_update"]); err != nil {
		return nil, fmt.Errorf("keep_secureboot_update: %w", err)
	}
	if o.allowShort, err = xapi.ParseBool(config["allow_incomplete_disks"]); err != nil {
		return nil, fmt.Errorf("allow_incomplete_disks: %w", err)
	}

	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Exporter{cfg: cfg, client: client, opts: o, config: config}, nil
}

func (p *Exporter) Origin() string        { return p.cfg.Origin() }
func (p *Exporter) Type() string          { return xapi.Protocol }
func (p *Exporter) Root() string          { return "/" }
func (p *Exporter) Flags() location.Flags { return 0 }

func (p *Exporter) Ping(ctx context.Context) error {
	return p.client.Call(ctx, nil, "pool.get_all")
}

func (p *Exporter) Close(ctx context.Context) error {
	var errs []error
	if p.watchdog != nil {
		errs = append(errs, p.watchdog.Close())
		p.watchdog = nil
	}
	errs = append(errs, p.client.Close(ctx))
	return errors.Join(errs...)
}

// restoredDisk is a VDI written from a disk record, and the size that record held.
type restoredDisk struct {
	ref     string
	size    int64
	stored  int64  // size of the stored file, which shortTail may round down to size
	kept    int64  // bytes of disk data written, size or less
	cut     []byte // the stored bytes from kept to the end of the file, if shortTail zeroed them
	dropped string // what of the stored file was not restored, "" if nothing
}

type pendingVM struct {
	record *connectors.Record
	dir    string
	meta   xapi.VMMeta
}

// Export restores each disk record into a new VDI as it arrives, then rebuilds each VM
// from its vm.json once every record is in, since nothing orders vm.json first.
func (p *Exporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	srRef, srErr := p.targetSR(ctx)
	sparse := false
	if srErr == nil {
		sparse, srErr = p.srZeroesNewVDIs(ctx, srRef)
	}

	restored := make(map[string]restoredDisk) // dir + "/" + original VDI UUID -> new VDI
	var vms []pendingVM
	var disks []doneDisk              // restored, reported once their VM's fate is known
	badMeta := make(map[string]error) // dir -> why its vm.json gave no VM to make

	for record := range records {
		if record.Err != nil || record.IsXattr || !record.FileInfo.Lmode.IsRegular() {
			results <- record.Ok()
			continue
		}

		dir, base := path.Split(record.Pathname)
		dir = path.Clean(dir)

		if err := ctx.Err(); err != nil {
			if base == xapi.MetaFilename {
				badMeta[dir] = err
			}
			results <- record.Error(err)
			continue
		}

		if base == xapi.MetaFilename {
			meta, err := readMeta(record)
			if err != nil {
				badMeta[dir] = err
				results <- result(record, err)
				continue
			}
			vms = append(vms, pendingVM{record: record, dir: dir, meta: meta})
			continue
		}

		vdiUUID, ok := xapi.ParseDiskFilename(base)
		if !ok {
			results <- record.Ok()
			continue
		}
		if srErr != nil {
			results <- record.Error(srErr)
			continue
		}

		disk, err := p.restoreDisk(ctx, srRef, sparse, base, record)
		if err != nil {
			results <- record.Error(err) // closes the reader
			continue
		}
		key := dir + "/" + vdiUUID
		restored[key] = disk
		// The next record comes only once this reader is closed (go-kloset-sdk,
		// receiveRecords), so it is closed now and the result sent later.
		_ = record.Close()
		record.Reader = nil
		disks = append(disks, doneDisk{record: record, key: key, dir: dir, ref: disk.ref})
	}

	failed := p.createVMs(ctx, vms, restored, sparse, results)
	owned := make(map[string]bool) // disks some vm.json lists
	for _, vm := range vms {
		for _, d := range vm.meta.Disks {
			owned[vm.dir+"/"+d.VDIUUID] = true
		}
	}

	// A disk is restored as part of its VM. One that no vm.json lists would be left
	// with no VM, so it goes; one whose VM failed is already gone with it. No path in
	// the messages: plakar prefixes every record error with it.
	for _, d := range disks {
		var err error
		switch {
		case owned[d.key] && failed[d.dir]:
			err = fmt.Errorf("its VM was not restored, see %s; the disk's VDI was destroyed with it", xapi.MetaFilename)
		case owned[d.key]:
		case badMeta[d.dir] != nil:
			err = fmt.Errorf("the %s beside this disk gave no VM to attach it to (%v): its VDI was destroyed", xapi.MetaFilename, badMeta[d.dir])
		case slices.ContainsFunc(vms, func(vm pendingVM) bool { return vm.dir == d.dir }):
			err = fmt.Errorf("the %s beside this disk does not list it: its VDI was destroyed", xapi.MetaFilename)
		default:
			err = fmt.Errorf("no %s in the restore for this disk, so no VM to attach it to: its VDI was destroyed", xapi.MetaFilename)
		}
		if err != nil && !owned[d.key] {
			if derr := p.destroyRef(ctx, cleanupRetry, "VDI.destroy", d.ref); derr != nil {
				err = fmt.Errorf("%w; destroying VDI %s failed: %w", err, d.ref, derr)
			}
		}
		results <- result(d.record, err)
	}
	return nil
}

type doneDisk struct {
	record *connectors.Record
	key    string // dir + "/" + original VDI UUID
	dir    string
	ref    string
}

// createVMs makes each VM from its vm.json and returns the directories of the VMs
// not restored. A VM the restore stops before has its restored disks destroyed, as
// when making it fails: they would have no owner. A VM held back from starting is
// restored, with its disks.
func (p *Exporter) createVMs(ctx context.Context, vms []pendingVM, restored map[string]restoredDisk, zeroed bool, results chan<- *connectors.Result) map[string]bool {
	failed := make(map[string]bool)
	for _, vm := range vms {
		err := ctx.Err()
		if err != nil {
			err = errors.Join(err, p.destroyDisks(ctx, vm, restored))
		} else {
			err = p.createVM(ctx, vm, restored, zeroed)
		}
		var held *heldError
		if err != nil && !errors.As(err, &held) {
			failed[vm.dir] = true
		}
		results <- result(vm.record, err)
	}
	return failed
}

func (p *Exporter) targetSR(ctx context.Context) (string, error) {
	var ref string
	if p.opts.srUUID != "" {
		if err := p.client.Call(ctx, &ref, "SR.get_by_uuid", p.opts.srUUID); err != nil {
			return "", err
		}
		return ref, nil
	}

	var pools []string
	if err := p.client.Call(ctx, &pools, "pool.get_all"); err != nil {
		return "", err
	}
	if len(pools) != 1 {
		return "", fmt.Errorf("expected one pool, found %d", len(pools))
	}
	if err := p.client.Call(ctx, &ref, "pool.get_default_SR", pools[0]); err != nil {
		return "", err
	}
	if ref == xapi.NullRef {
		return "", fmt.Errorf("the pool has no default SR: set sr_uuid")
	}
	return ref, nil
}

// srZeroesNewVDIs tells whether disks can be restored sparsely on the SR: only where
// a new VDI already reads as zeroes can the zero blocks be left out.
func (p *Exporter) srZeroesNewVDIs(ctx context.Context, srRef string) (bool, error) {
	var sr struct {
		Type     string            `json:"type"`
		SMConfig map[string]string `json:"sm_config"`
	}
	if err := p.client.Call(ctx, &sr, "SR.get_record", srRef); err != nil {
		return false, err
	}
	return xapi.SRZeroesNewVDIs(sr.Type, sr.SMConfig), nil
}

// restoreDisk creates a VDI and writes the record into it. With sparse set, only the
// blocks holding data are sent, so a thin SR allocates only those.
//
// With allow_incomplete_disks on such an SR, the end of the stored file goes through
// shortTail: a part sector and a trailing xapi error page are not disk data.
func (p *Exporter) restoreDisk(ctx context.Context, srRef string, sparse bool, name string, record *connectors.Record) (restoredDisk, error) {
	size := record.FileInfo.Lsize
	var r io.Reader = record.Reader
	var tail *shortTail
	if sparse && p.opts.allowShort {
		tail = newShortTail(r, size)
		r, size = tail, tail.limit
	}
	// Interrupted already: the create must not be sent, then searched for at length.
	if err := ctx.Err(); err != nil {
		return restoredDisk{}, err
	}
	var ref string
	label := restoreLabel()
	key := vdiLabelKey(srRef, label)
	p.watch(key)
	err := p.client.Call(ctx, &ref, "VDI.create", map[string]any{
		"name_label":       label,
		"name_description": "restored by plakar from " + name,
		"SR":               srRef,
		"virtual_size":     size,
		"type":             "user",
		"sharable":         false,
		"read_only":        false,
		"other_config":     map[string]string{},
		"xenstore_data":    map[string]string{},
		"sm_config":        map[string]string{},
		"tags":             []string{},
	})
	if err != nil {
		if mayHaveRun(err) {
			if cerr := p.destroyLabelledVDI(context.WithoutCancel(ctx), srRef, label); cerr != nil {
				// Still listed: the watchdog tries again when the plugin exits.
				return restoredDisk{}, errors.Join(err, fmt.Errorf("disk %s may be left behind as VDI %q: %w", name, label, cerr))
			}
		}
		p.unwatch(key)
		return restoredDisk{}, err
	}
	p.watch(vdiKey(ref))
	p.unwatch(key)
	// The label was needed only while the create had not answered. attach gives the
	// disk its original name; until then, and if a destroy later fails or the plugin is
	// killed, the file name says more than the label.
	if err := p.client.Call(ctx, nil, "VDI.set_name_label", ref, name); err != nil {
		warnf("disk %s: restored as VDI %q, renaming it failed: %s", name, label, err)
	}

	if sparse {
		_, err = p.client.ImportSparseVDI(ctx, ref, size, r)
	} else {
		err = p.client.ImportRawVDI(ctx, ref, size, r)
	}
	if err != nil {
		if derr := p.client.Call(context.WithoutCancel(ctx), nil, "VDI.destroy", ref); derr != nil {
			err = errors.Join(err, derr)
		} else {
			p.unwatch(vdiKey(ref))
		}
		return restoredDisk{}, err
	}
	disk := restoredDisk{ref: ref, size: size, stored: record.FileInfo.Lsize, kept: size}
	if tail != nil {
		disk.kept, disk.cut, disk.dropped = tail.kept, tail.cut, tail.dropped()
	}
	return disk, nil
}

// vmCreateFields are the VM record fields copied into VM.create. Run-time state,
// references to other objects and identity fields are left out.
var vmCreateFields = []string{
	"name_description", "user_version",
	"memory_target", "memory_static_max", "memory_dynamic_max", "memory_dynamic_min", "memory_static_min",
	"VCPUs_params", "VCPUs_max", "VCPUs_at_startup",
	"actions_after_softreboot", "actions_after_shutdown", "actions_after_reboot", "actions_after_crash",
	"PV_bootloader", "PV_kernel", "PV_ramdisk", "PV_args", "PV_bootloader_args", "PV_legacy_args",
	"HVM_boot_policy", "HVM_boot_params", "HVM_shadow_multiplier",
	"platform", "PCI_bus", "other_config", "recommendations", "xenstore_data",
	"start_delay", "shutdown_delay", "order",
	"hardware_platform_version", "has_vendor_device", "domain_type", "NVRAM",
}

func (p *Exporter) createVM(ctx context.Context, vm pendingVM, restored map[string]restoredDisk, zeroed bool) error {
	if err := p.checkDiskSizes(ctx, vm, restored, zeroed); err != nil {
		return err
	}
	src := vm.meta.VM
	rec := make(map[string]any, len(vmCreateFields)+8)
	for _, k := range vmCreateFields {
		if v, ok := src[k]; ok {
			rec[k] = v
		}
	}
	name, _ := src["name_label"].(string)
	rec["name_label"] = name + p.opts.nameSuffix
	rec["is_a_template"] = false
	// Without keep_tags the copy carries no tags, so a backup selecting VMs by tag
	// does not pick up the restored copy beside the original.
	rec["tags"] = []any{}
	if t, ok := src["tags"]; ok && p.opts.keepTags {
		rec["tags"] = t
	}
	rec["affinity"] = xapi.NullRef
	oc := map[string]any{}
	if m, ok := src["other_config"].(map[string]any); ok {
		maps.Copy(oc, m)
	}
	if !p.opts.keepMAC {
		// A copy beside the original must not derive the same MAC addresses.
		delete(oc, "mac_seed")
	}
	label := restoreLabel()
	oc[restoreMarkerKey] = label
	rec["other_config"] = oc

	// A VM that is not made leaves its restored disks with no owner, so they go too.
	if err := ctx.Err(); err != nil {
		return errors.Join(err, p.destroyDisks(ctx, vm, restored))
	}
	var vmRef string
	key := vmLabelKey(rec["name_label"].(string), label)
	p.watch(key)
	if err := p.client.Call(ctx, &vmRef, "VM.create", rec); err != nil {
		if mayHaveRun(err) {
			if cerr := p.destroyMarkedVM(context.WithoutCancel(ctx), rec["name_label"].(string), label); cerr != nil {
				// Still listed: the watchdog tries again when the plugin exits.
				err = errors.Join(err, fmt.Errorf("VM %q may be left behind, marked other_config:%s=%s: %w", rec["name_label"], restoreMarkerKey, label, cerr))
			} else {
				p.unwatch(key)
			}
		} else {
			p.unwatch(key)
		}
		return errors.Join(err, p.destroyDisks(ctx, vm, restored))
	}
	labPause("vm-created")
	p.watch(vmKey(vmRef))
	p.unwatch(key)

	if err := p.attach(ctx, vmRef, vm, restored); err != nil {
		return errors.Join(err, p.destroyVM(ctx, vmRef), p.destroyDisks(ctx, vm, restored))
	}
	labPause("attached")
	if err := p.client.Call(ctx, nil, "VM.remove_from_other_config", vmRef, restoreMarkerKey); err != nil {
		warnf("VM %q: removing other_config:%s: %s", name, restoreMarkerKey, err)
	}

	// XAPI copies never carry a unique vTPM's state; a restore does, by design.
	for _, t := range vm.meta.VTPMs {
		if t.IsUnique {
			warnf("VM %q: its vTPM is unique and the copy has the original's TPM state, so the same TPM identity and sealed secrets; do not run both", name)
		}
	}

	// Only the target pool can change a restored VM's Secure Boot state, so only a gap in
	// the pool holds the VM back; it is restored halted and the pool can be fixed first.
	if err := p.checkSecureBoot(ctx, vmRef, name); err != nil {
		var held *heldError
		if errors.As(err, &held) {
			p.keep(vmRef, vm, restored)
			labPause("kept")
			return err
		}
		// A VM whose Secure Boot state was not checked is not restored.
		return errors.Join(err, p.destroyVM(ctx, vmRef), p.destroyDisks(ctx, vm, restored))
	}
	// Kept from here, started or not: a VM.start that fails holds it back.
	p.keep(vmRef, vm, restored)
	labPause("kept")

	if p.opts.startOnRestore {
		if err := p.client.Call(ctx, nil, "VM.start", vmRef, false, false); err != nil {
			// The VM is complete, only not running: it and its disks are kept.
			return &heldError{fmt.Errorf("VM %q restored but not started: %w", name, err)}
		}
	}
	return nil
}

// checkSecureBoot runs after the VM is created and before it can start. The VM's NVRAM
// is restored as it was, so an initialised VM keeps its own Secure Boot keys whatever
// the target pool holds; only a VM that never booted takes its keys from the pool.
func (p *Exporter) checkSecureBoot(ctx context.Context, vmRef, name string) error {
	// XAPI marks a VM whose NVRAM holds only the 2011 Microsoft certificates for a key
	// update on boot when the target pool asks for it. That update replaces PK, KEK and
	// db, which changes what the guest measures into PCR7, so a key sealed in a restored
	// vTPM (BitLocker) would no longer unseal. A restore keeps the keys it saved.
	var certState string
	err := p.callRetry(ctx, &certState, "VM.get_secureboot_certificates_state", vmRef)
	switch {
	case xapi.IsCode(err, "MESSAGE_METHOD_UNKNOWN"):
		// Older XAPI: no certificate updates, nothing to undo.
	case err != nil:
		return err
	case certState == "update_on_boot" && p.opts.keepSBUpdate:
		warnf("VM %q: the target pool will replace its Secure Boot keys at next boot (keep_secureboot_update=true); a key sealed to its vTPM may not unseal", name)
	case certState == "update_on_boot":
		if err := p.callRetry(ctx, nil, "VM.update_secureboot_certificates_on_boot", vmRef, false); err != nil {
			return fmt.Errorf("VM %q: clearing the Secure Boot key update: %w", name, err)
		}
		warnf("VM %q keeps its 2011 Secure Boot certificates, which expire in 2026: the target pool's update to the 2023 ones was cleared to protect a key sealed to its vTPM; set keep_secureboot_update=true to keep the update", name)
	}

	labPause("secureboot")
	var readiness string
	err = p.callRetry(ctx, &readiness, "VM.get_secureboot_readiness", vmRef)
	if xapi.IsCode(err, "MESSAGE_METHOD_UNKNOWN") {
		return nil
	}
	if err != nil {
		return err
	}

	switch readiness {
	case "ready_no_dbx":
		warnf("VM %q: Secure Boot keys restored without dbx: revoked bootloaders would still boot", name)
	case "setup_mode":
		warnf("VM %q: Secure Boot is on but its NVRAM has no PK, so it is not enforced; the backed-up VM was in the same state", name)
	case "certs_incomplete":
		// The guest may still boot (a db entry is enough to verify its bootloader), and
		// starting the copy reproduces the original: not a reason to hold it back.
		warnf("VM %q: its NVRAM has a Secure Boot PK but lacks KEK or db; the backed-up VM was in the same state", name)
	case "first_boot":
		if reason, err := p.poolSecureBootGap(ctx); err != nil {
			return err
		} else if reason != "" {
			return &heldError{fmt.Errorf("VM %q restored but not started: it has never booted, so it takes its Secure Boot keys from the pool at first boot, and %s. Install the pool's Secure Boot certificates, then start it", name, reason)}
		}
	}
	return nil
}

// callRetry makes a call of the Secure Boot steps, retried while xapi does not answer.
// Their failure destroys a VM whose disks may have taken hours to copy, so a dropped
// connection must not count as one. Each call reads or sets a value, so a retry is safe.
func (p *Exporter) callRetry(ctx context.Context, out any, method string, args ...any) error {
	return p.retry(ctx, cleanupRetry, func(ctx context.Context, _ int) error {
		return p.client.Call(ctx, out, method, args...)
	})
}

// heldError is a VM restored in full but not started, held back for Secure Boot or
// failing VM.start: unlike any other error after VM.create, it keeps the VM and its
// disks, and their results report them restored.
type heldError struct{ error }

func (e *heldError) Unwrap() error { return e.error }

// destroyVM destroys a VM the restore made, even when ctx is done. VM.destroy
// removes the VM's VBDs but not their VDIs (xapi_vm_helpers.ml, destroy).
func (p *Exporter) destroyVM(ctx context.Context, vmRef string) error {
	return p.destroyRef(ctx, cleanupRetry, "VM.destroy", vmRef)
}

// poolSecureBootGap says why the target pool cannot give a first-booting VM its Secure
// Boot keys, or returns "" if it can.
func (p *Exporter) poolSecureBootGap(ctx context.Context) (string, error) {
	var pools []string
	if err := p.callRetry(ctx, &pools, "pool.get_all"); err != nil {
		return "", err
	}
	if len(pools) != 1 {
		return "", fmt.Errorf("expected one pool, found %d", len(pools))
	}
	var readiness string
	if err := p.callRetry(ctx, &readiness, "pool.get_guest_secureboot_readiness", pools[0]); err != nil {
		return "", err
	}
	if readiness == "not_ready" {
		return "the pool has no complete set of Secure Boot certificates (PK, KEK, db)", nil
	}

	// The readiness above is read on the pool master only; the VM may start on any host.
	var hosts []string
	if err := p.callRetry(ctx, &hosts, "host.get_all"); err != nil {
		return "", err
	}
	var first map[string]string
	for i, h := range hosts {
		var field string
		if err := p.callRetry(ctx, &field, "host.get_uefi_certificates", h); err != nil {
			return "", err
		}
		certs, err := xapi.ParseUEFICertificates(field)
		if err != nil {
			return "", fmt.Errorf("host Secure Boot certificates: %w", err)
		}
		if i == 0 {
			first = certs
		} else if !maps.Equal(certs, first) {
			return "the pool's hosts hold different Secure Boot certificates", nil
		}
	}
	return "", nil
}

// warnf reports a condition that does not fail the restore. Plugin results carry only
// success or an error, so warnings go to standard error.
func warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "xcpng: warning: "+format+"\n", args...)
}

// checkDiskSizes refuses a VM whose restored disks do not have the virtual size its
// vm.json records: a backup made before #27 was fixed can hold a disk whose export
// was cut short. Every disk restored for that VM is then destroyed, so the failed
// restore leaves nothing behind.
//
// With allow_incomplete_disks, a disk that is complete after all gets back the end
// shortTail took for an error page, and a short disk is grown to its recorded size
// instead and the VM is restored with a warning. The grown part must read as zeroes, which only an
// SR that zeroes new VDIs (zeroed) guarantees, so elsewhere the disk is still refused.
func (p *Exporter) checkDiskSizes(ctx context.Context, vm pendingVM, restored map[string]restoredDisk, zeroed bool) error {
	var errs []error
	for _, d := range vm.meta.Disks {
		disk, ok := restored[vm.dir+"/"+d.VDIUUID]
		if !ok {
			continue
		}
		if disk.cut != nil && disk.stored == d.VirtualSize {
			// The disk is complete, so what shortTail took for xapi's error page
			// is the guest's own data: put it back as it was stored.
			if err := p.client.WriteVDIAt(ctx, disk.ref, disk.kept, disk.cut); err != nil {
				errs = append(errs, fmt.Errorf("writing back the last %d bytes of disk %s of %s: %w", len(disk.cut), d.File, vm.dir, err))
			}
			continue
		}
		if disk.size == d.VirtualSize {
			if disk.dropped != "" {
				warnf("disk %s of %s: %s (allow_incomplete_disks=true)", d.File, vm.dir, disk.dropped)
			}
			continue
		}
		if disk.size > d.VirtualSize || !p.opts.allowShort {
			errs = append(errs, fmt.Errorf("disk %s of %s holds %d bytes, vm.json says %d: the backup is incomplete",
				d.File, vm.dir, disk.size, d.VirtualSize))
			continue
		}
		if !zeroed {
			errs = append(errs, fmt.Errorf("disk %s of %s holds %d bytes, vm.json says %d: allow_incomplete_disks needs an SR whose new disks read as zeroes",
				d.File, vm.dir, disk.size, d.VirtualSize))
			continue
		}
		if err := p.client.Call(ctx, nil, "VDI.resize", disk.ref, d.VirtualSize); err != nil {
			errs = append(errs, fmt.Errorf("growing disk %s of %s to %d bytes: %w", d.File, vm.dir, d.VirtualSize, err))
			continue
		}
		note := ""
		if disk.dropped != "" {
			note = "; " + disk.dropped
		}
		warnf("disk %s of %s holds only %d of its %d bytes (the backup is incomplete): restored with the last %d bytes read as zeroes%s (allow_incomplete_disks=true)",
			d.File, vm.dir, disk.kept, d.VirtualSize, d.VirtualSize-disk.kept, note)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(append(errs, p.destroyDisks(ctx, vm, restored))...)
}

// destroyDisks destroys every disk restored for a VM that is not restored, even when
// ctx is done: an interrupted restore must not leave them behind either.
func (p *Exporter) destroyDisks(ctx context.Context, vm pendingVM, restored map[string]restoredDisk) error {
	var errs []error
	for _, d := range vm.meta.Disks {
		disk, ok := restored[vm.dir+"/"+d.VDIUUID]
		if !ok {
			continue
		}
		if err := p.destroyRef(ctx, cleanupRetry, "VDI.destroy", disk.ref); err != nil {
			errs = append(errs, fmt.Errorf("destroying restored VDI %s: %w", disk.ref, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Exporter) attach(ctx context.Context, vmRef string, vm pendingVM, restored map[string]restoredDisk) error {
	for _, d := range vm.meta.Disks {
		disk, ok := restored[vm.dir+"/"+d.VDIUUID]
		vdiRef := disk.ref
		if !ok {
			return fmt.Errorf("disk %s of %s was not restored", d.File, vm.dir)
		}
		if err := p.client.Call(ctx, nil, "VDI.set_name_label", vdiRef, d.NameLabel); err != nil {
			return err
		}
		if err := p.client.Call(ctx, nil, "VDI.set_name_description", vdiRef, d.NameDescription); err != nil {
			return err
		}
		err := p.client.Call(ctx, nil, "VBD.create", map[string]any{
			"VM":                   vmRef,
			"VDI":                  vdiRef,
			"userdevice":           d.Userdevice,
			"bootable":             d.Bootable,
			"mode":                 d.Mode,
			"type":                 "Disk",
			"unpluggable":          false,
			"empty":                false,
			"other_config":         map[string]string{},
			"qos_algorithm_type":   "",
			"qos_algorithm_params": map[string]string{},
		})
		if err != nil {
			return err
		}
	}

	for _, v := range vm.meta.VIFs {
		netRef, err := p.findNetwork(ctx, v)
		if err != nil {
			return err
		}
		mac := "" // XAPI generates one
		if p.opts.keepMAC && !v.MACAutogenerated {
			mac = v.MAC
		}
		err = p.client.Call(ctx, nil, "VIF.create", map[string]any{
			"device":               v.Device,
			"network":              netRef,
			"VM":                   vmRef,
			"MAC":                  mac,
			"MTU":                  v.MTU,
			"other_config":         map[string]string{},
			"qos_algorithm_type":   "",
			"qos_algorithm_params": map[string]string{},
			"locking_mode":         "network_default",
			"ipv4_allowed":         []string{},
			"ipv6_allowed":         []string{},
		})
		if err != nil {
			return err
		}
	}

	// A guest that sealed secrets to its TPM cannot unseal them without this state.
	for _, t := range vm.meta.VTPMs {
		var ref string
		if err := p.client.Call(ctx, &ref, "VTPM.create", vmRef, t.IsUnique); err != nil {
			return fmt.Errorf("vTPM of %s: %w", vm.dir, err)
		}
		if err := p.client.Call(ctx, nil, "VTPM.set_contents", ref, t.Contents); err != nil {
			return fmt.Errorf("vTPM contents of %s: %w", vm.dir, err)
		}
	}
	return nil
}

func (p *Exporter) findNetwork(ctx context.Context, v xapi.VIFMeta) (string, error) {
	var ref string
	err := p.client.Call(ctx, &ref, "network.get_by_uuid", v.NetworkUUID)
	if err == nil {
		return ref, nil
	}
	if !xapi.IsCode(err, "UUID_INVALID") {
		return "", err
	}

	var refs []string
	if err := p.client.Call(ctx, &refs, "network.get_by_name_label", v.NetworkName); err != nil {
		return "", err
	}
	if len(refs) != 1 {
		return "", fmt.Errorf("VIF %s: network %s not found by UUID, and %d networks are named %q", v.Device, v.NetworkUUID, len(refs), v.NetworkName)
	}
	return refs[0], nil
}

func readMeta(record *connectors.Record) (xapi.VMMeta, error) {
	var meta xapi.VMMeta
	if record.Reader == nil {
		return meta, fmt.Errorf("%s: no data", record.Pathname)
	}
	data, err := io.ReadAll(io.LimitReader(record.Reader, maxMetaSize+1))
	_ = record.Close()
	record.Reader = nil
	if err != nil {
		return meta, fmt.Errorf("%s: %w", record.Pathname, err)
	}
	if len(data) > maxMetaSize {
		return meta, fmt.Errorf("%s: larger than %d bytes", record.Pathname, maxMetaSize)
	}

	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&meta); err != nil {
		return meta, fmt.Errorf("%s: %w", record.Pathname, err)
	}
	if meta.Version != xapi.MetaVersion {
		return meta, fmt.Errorf("%s: unsupported version %d", record.Pathname, meta.Version)
	}
	return meta, nil
}

func result(record *connectors.Record, err error) *connectors.Result {
	if record.Reader != nil {
		_ = record.Close()
		record.Reader = nil
	}
	return &connectors.Result{Record: *record, Err: err}
}
