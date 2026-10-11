package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"

	"integration-xcpng/internal/xapi"
)

func init() {
	if err := importer.Register(xapi.Protocol, 0, NewImporter); err != nil {
		panic(err)
	}
}

type selection struct {
	uuid string
	name string
	tag  string
}

// snapshotMarker is set in the other_config of every snapshot this connector takes,
// so that leftovers from an interrupted backup can be found and destroyed, without
// touching snapshots someone else took.
const snapshotMarker = "plakar_xcpng_snapshot"

// defaultStaleAge is how old a marked snapshot must be before a later backup treats it
// as a leftover. It must exceed the longest backup of one VM, or two overlapping
// backups of the same VM would destroy each other's snapshot.
const defaultStaleAge = 24 * time.Hour

type Importer struct {
	cfg      *xapi.Config
	client   *xapi.Client
	sel      selection
	staleAge time.Duration

	// snapshots left to destroy in Close(), when no acknowledgements were available
	// to tell when their records had been read.
	deferred []string
}

func NewImporter(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (importer.Importer, error) {
	cfg, err := xapi.ParseConfig(config)
	if err != nil {
		return nil, err
	}

	sel := selection{
		uuid: strings.TrimSpace(config["vm_uuid"]),
		name: strings.TrimSpace(config["vm_name"]),
		tag:  strings.TrimSpace(config["vm_tag"]),
	}
	n := 0
	for _, v := range []string{sel.uuid, sel.name, sel.tag} {
		if v != "" {
			n++
		}
	}
	if n != 1 {
		return nil, fmt.Errorf("exactly one of vm_uuid, vm_name or vm_tag must be set")
	}

	staleAge := defaultStaleAge
	if v := strings.TrimSpace(config["stale_snapshot_age"]); v != "" {
		staleAge, err = time.ParseDuration(v)
		if err != nil || staleAge < 0 {
			return nil, fmt.Errorf("invalid stale_snapshot_age %q: a Go duration such as 24h, or 0 to disable", v)
		}
	}

	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &Importer{cfg: cfg, client: client, sel: sel, staleAge: staleAge}, nil
}

func (p *Importer) Origin() string { return p.cfg.Origin() }
func (p *Importer) Type() string   { return xapi.Protocol }
func (p *Importer) Root() string   { return "/" }
func (p *Importer) Flags() location.Flags {
	return location.FLAG_STREAM | location.FLAG_NEEDACK
}

func (p *Importer) Ping(ctx context.Context) error {
	return p.client.Call(ctx, nil, "pool.get_all")
}

func (p *Importer) Close(ctx context.Context) error {
	var errs []error
	for _, snap := range p.deferred {
		errs = append(errs, p.destroySnapshot(ctx, snap))
	}
	p.deferred = nil
	errs = append(errs, p.client.Close(ctx))
	return errors.Join(errs...)
}

func (p *Importer) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	defer close(records)

	var acks *ackTracker
	if results != nil {
		acks = newAckTracker()
		go acks.run(results)
	}

	vms, err := p.resolveVMs(ctx)
	if err != nil {
		return err
	}
	if len(vms) == 0 {
		return fmt.Errorf("no VM matches the selection")
	}

	for _, vm := range vms {
		if err := p.backupVM(ctx, vm, records, acks); err != nil {
			return err
		}
	}
	return nil
}

type vmSummary struct {
	IsATemplate     bool     `json:"is_a_template"`
	IsASnapshot     bool     `json:"is_a_snapshot"`
	IsControlDomain bool     `json:"is_control_domain"`
	Tags            []string `json:"tags"`
}

func (s vmSummary) isGuest() bool {
	return !s.IsATemplate && !s.IsASnapshot && !s.IsControlDomain
}

func (p *Importer) resolveVMs(ctx context.Context) ([]string, error) {
	switch {
	case p.sel.uuid != "":
		var ref string
		if err := p.client.Call(ctx, &ref, "VM.get_by_uuid", p.sel.uuid); err != nil {
			return nil, err
		}
		return []string{ref}, nil

	case p.sel.name != "":
		var refs []string
		if err := p.client.Call(ctx, &refs, "VM.get_by_name_label", p.sel.name); err != nil {
			return nil, err
		}
		var out []string
		for _, ref := range refs {
			var s vmSummary
			if err := p.client.Call(ctx, &s, "VM.get_record", ref); err != nil {
				return nil, err
			}
			if s.isGuest() {
				out = append(out, ref)
			}
		}
		return out, nil

	default:
		var all map[string]vmSummary
		if err := p.client.Call(ctx, &all, "VM.get_all_records"); err != nil {
			return nil, err
		}
		var out []string
		for ref, s := range all {
			if s.isGuest() && slices.Contains(s.Tags, p.sel.tag) {
				out = append(out, ref)
			}
		}
		slices.Sort(out)
		return out, nil
	}
}

type snapshotDisk struct {
	meta    xapi.DiskMeta
	snapVDI string
}

func (p *Importer) backupVM(ctx context.Context, vmRef string, records chan<- *connectors.Record, acks *ackTracker) (err error) {
	var vm map[string]any
	if err := p.client.Call(ctx, &vm, "VM.get_record", vmRef); err != nil {
		return err
	}
	vmUUID, _ := vm["uuid"].(string)
	if vmUUID == "" {
		return fmt.Errorf("VM %s has no uuid", vmRef)
	}

	if err := p.destroyStaleSnapshots(ctx, vmRef); err != nil {
		return fmt.Errorf("VM %s: %w", vmUUID, err)
	}

	var snapRef string
	label := fmt.Sprintf("plakar-%s", time.Now().UTC().Format("20060102T150405Z"))
	if err := p.client.Call(ctx, &snapRef, "VM.snapshot", vmRef, label); err != nil {
		return fmt.Errorf("snapshot of VM %s: %w", vmUUID, err)
	}
	// The marker holds the snapshot's own UUID: other_config is copied by clone and
	// revert, and a copied marker must not make another snapshot look like ours.
	var snapUUID string
	if err := p.client.Call(ctx, &snapUUID, "VM.get_uuid", snapRef); err != nil {
		return errors.Join(fmt.Errorf("snapshot of VM %s: %w", vmUUID, err),
			p.destroySnapshot(context.WithoutCancel(ctx), snapRef))
	}
	if err := p.client.Call(ctx, nil, "VM.add_to_other_config", snapRef, snapshotMarker, snapUUID); err != nil {
		return errors.Join(fmt.Errorf("marking snapshot of VM %s: %w", vmUUID, err),
			p.destroySnapshot(context.WithoutCancel(ctx), snapRef))
	}

	var pending []<-chan error
	defer func() {
		if acks == nil {
			// Nothing tells us when the streams are done: keep the snapshot until Close().
			p.deferred = append(p.deferred, snapRef)
			return
		}
		for _, ch := range pending {
			<-ch
		}
		// The caller's context may be cancelled; cleanup must still happen.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		if cerr := p.destroySnapshot(cleanupCtx, snapRef); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	var snapTimeStr string
	if err := p.client.Call(ctx, &snapTimeStr, "VM.get_snapshot_time", snapRef); err != nil {
		return err
	}
	modTime, terr := xapi.ParseTime(snapTimeStr)
	if terr != nil {
		modTime = time.Now()
	}

	disks, err := p.snapshotDisks(ctx, snapRef)
	if err != nil {
		return err
	}
	vifs, err := p.vifs(ctx, vmRef)
	if err != nil {
		return err
	}

	meta := xapi.VMMeta{Version: xapi.MetaVersion, VM: vm, VIFs: vifs}
	for _, d := range disks {
		meta.Disks = append(meta.Disks, d.meta)
	}
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	dir := path.Join("/", vmUUID)
	emit := func(rec *connectors.Record) error {
		var ch <-chan error
		if acks != nil {
			ch = acks.expect(rec.Pathname)
		}
		select {
		case <-ctx.Done():
			if acks != nil {
				acks.forget(rec.Pathname)
			}
			_ = rec.Close()
			return ctx.Err()
		case records <- rec:
			if ch != nil {
				pending = append(pending, ch)
			}
			return nil
		}
	}

	for _, d := range disks {
		snapVDI := d.snapVDI
		rec := connectors.NewRecord(
			path.Join(dir, d.meta.File), "",
			objects.FileInfo{
				Lname:    d.meta.File,
				Lsize:    d.meta.VirtualSize,
				Lmode:    0600,
				LmodTime: modTime,
				Ldev:     1,
			},
			nil,
			func() (io.ReadCloser, error) { return p.client.ExportRawVDI(ctx, snapVDI) },
		)
		if err := emit(rec); err != nil {
			return err
		}
	}

	return emit(&connectors.Record{
		Pathname: path.Join(dir, xapi.MetaFilename),
		FileInfo: objects.FileInfo{
			Lname:    xapi.MetaFilename,
			Lsize:    int64(len(metaData)),
			Lmode:    0600,
			LmodTime: modTime,
			Ldev:     1,
		},
		Reader: io.NopCloser(bytes.NewReader(metaData)),
	})
}

// snapshotDisks lists the disk VBDs of a snapshot, named after the VDIs they were
// snapshotted from.
func (p *Importer) snapshotDisks(ctx context.Context, snapRef string) ([]snapshotDisk, error) {
	var vbds []string
	if err := p.client.Call(ctx, &vbds, "VM.get_VBDs", snapRef); err != nil {
		return nil, err
	}

	var out []snapshotDisk
	for _, vbdRef := range vbds {
		var vbd xapi.VBDRecord
		if err := p.client.Call(ctx, &vbd, "VBD.get_record", vbdRef); err != nil {
			return nil, err
		}
		if vbd.Type != "Disk" || vbd.Empty || vbd.VDI == xapi.NullRef {
			continue
		}

		var vdi xapi.VDIRecord
		if err := p.client.Call(ctx, &vdi, "VDI.get_record", vbd.VDI); err != nil {
			return nil, err
		}

		origUUID := vdi.UUID
		if vdi.SnapshotOf != "" && vdi.SnapshotOf != xapi.NullRef {
			if err := p.client.Call(ctx, &origUUID, "VDI.get_uuid", vdi.SnapshotOf); err != nil {
				return nil, err
			}
		}
		var srUUID string
		if err := p.client.Call(ctx, &srUUID, "SR.get_uuid", vdi.SR); err != nil {
			return nil, err
		}

		out = append(out, snapshotDisk{
			snapVDI: vbd.VDI,
			meta: xapi.DiskMeta{
				File:            xapi.DiskFilename(vbd.Userdevice, origUUID),
				VDIUUID:         origUUID,
				NameLabel:       vdi.NameLabel,
				NameDescription: vdi.NameDescription,
				VirtualSize:     vdi.VirtualSize,
				SRUUID:          srUUID,
				Userdevice:      vbd.Userdevice,
				Bootable:        vbd.Bootable,
				Mode:            vbd.Mode,
			},
		})
	}
	slices.SortFunc(out, func(a, b snapshotDisk) int { return strings.Compare(a.meta.File, b.meta.File) })
	return out, nil
}

func (p *Importer) vifs(ctx context.Context, vmRef string) ([]xapi.VIFMeta, error) {
	var refs []string
	if err := p.client.Call(ctx, &refs, "VM.get_VIFs", vmRef); err != nil {
		return nil, err
	}

	var out []xapi.VIFMeta
	for _, ref := range refs {
		var vif xapi.VIFRecord
		if err := p.client.Call(ctx, &vif, "VIF.get_record", ref); err != nil {
			return nil, err
		}
		var net xapi.NetworkRecord
		if err := p.client.Call(ctx, &net, "network.get_record", vif.Network); err != nil {
			return nil, err
		}
		out = append(out, xapi.VIFMeta{
			Device:           vif.Device,
			MAC:              vif.MAC,
			MACAutogenerated: vif.MACAutogenerated,
			MTU:              vif.MTU,
			NetworkUUID:      net.UUID,
			NetworkName:      net.NameLabel,
		})
	}
	slices.SortFunc(out, func(a, b xapi.VIFMeta) int { return strings.Compare(a.Device, b.Device) })
	return out, nil
}

type snapshotSummary struct {
	UUID         string            `json:"uuid"`
	OtherConfig  map[string]string `json:"other_config"`
	SnapshotTime string            `json:"snapshot_time"`
}

// destroyStaleSnapshots destroys this connector's snapshots of a VM that are older
// than staleAge: what an interrupted backup leaves behind.
func (p *Importer) destroyStaleSnapshots(ctx context.Context, vmRef string) error {
	if p.staleAge == 0 {
		return nil
	}
	var snaps []string
	if err := p.client.Call(ctx, &snaps, "VM.get_snapshots", vmRef); err != nil {
		return err
	}
	var errs []error
	for _, snap := range snaps {
		var s snapshotSummary
		if err := p.client.Call(ctx, &s, "VM.get_record", snap); err != nil {
			errs = append(errs, err)
			continue
		}
		if s.OtherConfig[snapshotMarker] != s.UUID || s.UUID == "" {
			continue
		}
		t, err := xapi.ParseTime(s.SnapshotTime)
		if err != nil || time.Since(t) < p.staleAge {
			continue
		}
		errs = append(errs, p.destroySnapshot(ctx, snap))
	}
	return errors.Join(errs...)
}

// destroySnapshot destroys a VM snapshot and its disk VDIs. VM.destroy alone leaves
// the VDIs behind.
func (p *Importer) destroySnapshot(ctx context.Context, snapRef string) error {
	var vbds []string
	if err := p.client.Call(ctx, &vbds, "VM.get_VBDs", snapRef); err != nil {
		return fmt.Errorf("cleanup of snapshot %s: %w", snapRef, err)
	}
	var vdis []string
	for _, vbdRef := range vbds {
		var vbd xapi.VBDRecord
		if err := p.client.Call(ctx, &vbd, "VBD.get_record", vbdRef); err != nil {
			return fmt.Errorf("cleanup of snapshot %s: %w", snapRef, err)
		}
		if vbd.Type != "Disk" || vbd.VDI == xapi.NullRef {
			continue
		}
		// Only snapshot copies: never a VDI the snapshot merely shares with a VM.
		var isSnap bool
		if err := p.client.Call(ctx, &isSnap, "VDI.get_is_a_snapshot", vbd.VDI); err != nil {
			return fmt.Errorf("cleanup of snapshot %s: %w", snapRef, err)
		}
		if isSnap {
			vdis = append(vdis, vbd.VDI)
		}
	}

	if err := p.client.Call(ctx, nil, "VM.destroy", snapRef); err != nil {
		return fmt.Errorf("cleanup of snapshot %s: %w", snapRef, err)
	}
	var errs []error
	for _, vdi := range vdis {
		if err := p.client.Call(ctx, nil, "VDI.destroy", vdi); err != nil {
			errs = append(errs, fmt.Errorf("cleanup of snapshot VDI %s: %w", vdi, err))
		}
	}
	return errors.Join(errs...)
}
