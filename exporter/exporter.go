package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"

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
}

type Exporter struct {
	cfg    *xapi.Config
	client *xapi.Client
	opts   options
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

	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Exporter{cfg: cfg, client: client, opts: o}, nil
}

func (p *Exporter) Origin() string        { return p.cfg.Origin() }
func (p *Exporter) Type() string          { return xapi.Protocol }
func (p *Exporter) Root() string          { return "/" }
func (p *Exporter) Flags() location.Flags { return 0 }

func (p *Exporter) Ping(ctx context.Context) error {
	return p.client.Call(ctx, nil, "pool.get_all")
}

func (p *Exporter) Close(ctx context.Context) error {
	return p.client.Close(ctx)
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

	restored := make(map[string]string) // dir + "/" + original VDI UUID -> new VDI ref
	var vms []pendingVM

	for record := range records {
		if record.Err != nil || record.IsXattr || !record.FileInfo.Lmode.IsRegular() {
			results <- record.Ok()
			continue
		}
		if err := ctx.Err(); err != nil {
			results <- record.Error(err)
			continue
		}

		dir, base := path.Split(record.Pathname)
		dir = path.Clean(dir)

		if base == xapi.MetaFilename {
			meta, err := readMeta(record)
			if err != nil {
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

		ref, err := p.restoreDisk(ctx, srRef, base, record)
		if err == nil {
			restored[dir+"/"+vdiUUID] = ref
		}
		results <- record.Error(err) // Error(nil) closes the reader and reports success
	}

	for _, vm := range vms {
		if err := ctx.Err(); err != nil {
			results <- result(vm.record, err)
			continue
		}
		results <- result(vm.record, p.createVM(ctx, vm, restored))
	}
	return nil
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

func (p *Exporter) restoreDisk(ctx context.Context, srRef, name string, record *connectors.Record) (string, error) {
	size := record.FileInfo.Lsize
	var ref string
	err := p.client.Call(ctx, &ref, "VDI.create", map[string]any{
		"name_label":       name,
		"name_description": "restored by plakar",
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
		return "", err
	}

	if err := p.client.ImportRawVDI(ctx, ref, size, record.Reader); err != nil {
		if derr := p.client.Call(context.WithoutCancel(ctx), nil, "VDI.destroy", ref); derr != nil {
			err = errors.Join(err, derr)
		}
		return "", err
	}
	return ref, nil
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

func (p *Exporter) createVM(ctx context.Context, vm pendingVM, restored map[string]string) error {
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
	if oc, ok := rec["other_config"].(map[string]any); ok && !p.opts.keepMAC {
		// A copy beside the original must not derive the same MAC addresses.
		delete(oc, "mac_seed")
	}

	var vmRef string
	if err := p.client.Call(ctx, &vmRef, "VM.create", rec); err != nil {
		return err
	}

	if err := p.attach(ctx, vmRef, vm, restored); err != nil {
		if derr := p.client.Call(context.WithoutCancel(ctx), nil, "VM.destroy", vmRef); derr != nil {
			err = errors.Join(err, derr)
		}
		return err
	}

	if p.opts.startOnRestore {
		return p.client.Call(ctx, nil, "VM.start", vmRef, false, false)
	}
	return nil
}

func (p *Exporter) attach(ctx context.Context, vmRef string, vm pendingVM, restored map[string]string) error {
	for _, d := range vm.meta.Disks {
		vdiRef, ok := restored[vm.dir+"/"+d.VDIUUID]
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
