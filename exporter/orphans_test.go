package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"

	"integration-xcpng/internal/xapi"
)

var orphansDisks = map[string]restoredDisk{
	"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
	"/vm/b": {ref: "OpaqueRef:b", size: 8192, kept: 8192},
}

// wantDisksDestroyed fails unless both restored disks of sizesVM were destroyed.
func wantDisksDestroyed(t *testing.T, f *destroyRecorder) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	got := slices.Sorted(slices.Values(f.destroyed))
	if !slices.Equal(got, []string{"OpaqueRef:a", "OpaqueRef:b"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:a and OpaqueRef:b", f.destroyed)
	}
}

func TestCreateVMFailureDestroysDisks(t *testing.T) {
	f := &destroyRecorder{} // VM.create fails
	p := sizesExporter(t, f, options{})

	if err := p.createVM(context.Background(), sizesVM, orphansDisks, true); err == nil {
		t.Fatal("createVM succeeded although VM.create failed")
	}
	wantDisksDestroyed(t, f)
}

func TestAttachFailureDestroysVMAndDisks(t *testing.T) {
	f := &destroyRecorder{vmRef: "OpaqueRef:vm"} // VBD.create fails
	p := sizesExporter(t, f, options{})

	if err := p.createVM(context.Background(), sizesVM, orphansDisks, true); err == nil {
		t.Fatal("createVM succeeded although VBD.create failed")
	}
	if !slices.Equal(f.vmDestroyed, []string{"OpaqueRef:vm"}) {
		t.Fatalf("destroyed VMs %v, want OpaqueRef:vm", f.vmDestroyed)
	}
	// VM.destroy removes the VBDs, never the VDIs (xapi_vm_helpers.ml, destroy).
	wantDisksDestroyed(t, f)
}

func TestCheckDiskSizesDestroysDisksWhenInterrupted(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	short := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 1000, kept: 1000},
	}
	if err := p.checkDiskSizes(ctx, sizesVM, short, true); err == nil {
		t.Fatal("short disk not refused")
	}
	wantDisksDestroyed(t, f)
}

// restoreXAPI fakes the calls of a restore whose VDI.create or VM.create xapi carries
// out but never answers when drop* is set: the connection is closed instead. With
// refuse*, the call fails with a xapi error and creates nothing. The SR reports a
// vdi_create operation on the first busyLooks reads of its current operations.
type restoreXAPI struct {
	mu         sync.Mutex
	dropVDI    bool
	refuseVDI  bool
	dropVM     bool
	busyLooks  int
	vdiLabels  map[string]string // ref -> name_label
	vmMarkers  map[string]string // ref -> other_config marker
	destroyed  []string          // VDIs and VMs, in order
	unmarked   []string
	lookups    int // VDI.get_by_name_label and VM.get_by_name_label calls
	srOpsReads int
	// dropSB drops VM.get_secureboot_certificates_state. sbReadiness, when set, is the
	// VM's Secure Boot readiness, on a pool whose own readiness is not_ready.
	dropSB      bool
	sbReadiness string
}

func (f *restoreXAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/import_raw_vdi" {
		(&destroyRecorder{}).recordImport(w, r)
		return
	}
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { // reads the body before any drop
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	str := func(i int) string {
		var s string
		_ = json.Unmarshal(req.Params[i], &s)
		return s
	}
	xapiErr := func(code string) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": code, "data": []string{}},
		})
	}
	drop := func() {
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			_ = conn.Close()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result any = ""
	switch req.Method {
	case "session.login_with_password":
		result = "OpaqueRef:session"
	case "task.create":
		result = "OpaqueRef:task"
	case "task.get_status":
		result = "success"
	case "task.destroy":
	case "VDI.create":
		if f.refuseVDI {
			xapiErr("SR_BACKEND_FAILURE")
			return
		}
		var rec struct {
			NameLabel string `json:"name_label"`
		}
		_ = json.Unmarshal(req.Params[1], &rec)
		ref := fmt.Sprintf("OpaqueRef:vdi%d", len(f.vdiLabels))
		if f.vdiLabels == nil {
			f.vdiLabels = map[string]string{}
		}
		f.vdiLabels[ref] = rec.NameLabel
		if f.dropVDI {
			drop()
			return
		}
		result = ref
	case "SR.get_current_operations":
		f.srOpsReads++
		ops := map[string]string{}
		if f.srOpsReads <= f.busyLooks {
			ops["OpaqueRef:task"] = "vdi_create"
		}
		result = ops
	case "VDI.get_by_name_label":
		f.lookups++
		refs := []string{}
		for ref, l := range f.vdiLabels {
			if l == str(1) {
				refs = append(refs, ref)
			}
		}
		result = refs
	case "VDI.get_SR":
		result = "OpaqueRef:sr"
	case "VDI.destroy", "VM.destroy":
		f.destroyed = append(f.destroyed, str(1))
	case "VDI.set_name_label":
		if f.vdiLabels == nil {
			f.vdiLabels = map[string]string{}
		}
		f.vdiLabels[str(1)] = str(2)
	case "VDI.set_name_description", "VBD.create":
	case "VM.create":
		var rec struct {
			OtherConfig map[string]string `json:"other_config"`
		}
		_ = json.Unmarshal(req.Params[1], &rec)
		if f.vmMarkers == nil {
			f.vmMarkers = map[string]string{"OpaqueRef:samename": ""}
		}
		ref := fmt.Sprintf("OpaqueRef:vm%d", len(f.vmMarkers))
		f.vmMarkers[ref] = rec.OtherConfig[restoreMarkerKey]
		if f.dropVM {
			drop()
			return
		}
		result = ref
	case "VM.get_by_name_label":
		f.lookups++
		refs := []string{}
		for ref := range f.vmMarkers {
			refs = append(refs, ref)
		}
		result = refs
	case "VM.get_other_config":
		result = map[string]string{restoreMarkerKey: f.vmMarkers[str(1)]}
	case "VM.remove_from_other_config":
		f.unmarked = append(f.unmarked, str(1)+" "+str(2))
	case "VM.get_secureboot_certificates_state":
		if f.dropSB {
			drop()
			return
		}
		result = "ok"
	case "VM.get_secureboot_readiness":
		if f.sbReadiness == "" {
			xapiErr("MESSAGE_METHOD_UNKNOWN")
			return
		}
		result = f.sbReadiness
	case "pool.get_all":
		result = []string{"OpaqueRef:pool"}
	case "pool.get_guest_secureboot_readiness":
		result = "not_ready"
	case "SR.get_by_uuid":
		result = "OpaqueRef:sr"
	case "SR.get_record":
		result = map[string]any{"type": "lvm", "sm_config": map[string]string{}}
	default:
		xapiErr("MESSAGE_METHOD_UNKNOWN")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func restoreExporter(t *testing.T, f *restoreXAPI) *Exporter {
	t.Helper()
	old := restorePollDelay
	restorePollDelay = time.Millisecond
	t.Cleanup(func() { restorePollDelay = old })
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	c, err := xapi.Dial(context.Background(), &xapi.Config{
		Host:        strings.TrimPrefix(srv.URL, "https://"),
		TLSInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Exporter{client: c}
}

func diskRecord(data []byte) *connectors.Record {
	return &connectors.Record{
		Pathname: "/vm/disk-0",
		FileInfo: objects.FileInfo{Lsize: int64(len(data))},
		Reader:   io.NopCloser(bytes.NewReader(data)),
	}
}

func TestUnansweredVDICreateIsDestroyed(t *testing.T) {
	f := &restoreXAPI{dropVDI: true, busyLooks: 2}
	p := restoreExporter(t, f)

	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err == nil {
		t.Fatal("restoreDisk succeeded although VDI.create never answered")
	}
	if f.srOpsReads <= f.busyLooks {
		t.Fatalf("looked up the VDI after %d reads of the SR operations, before vdi_create ended", f.srOpsReads)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi0", f.destroyed)
	}
	if l := f.vdiLabels["OpaqueRef:vdi0"]; !strings.HasPrefix(l, "plakar-restore-") {
		t.Fatalf("VDI created with label %q, not a unique restore label", l)
	}
}

func TestRefusedVDICreateIsNotLookedFor(t *testing.T) {
	f := &restoreXAPI{refuseVDI: true}
	p := restoreExporter(t, f)

	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err == nil {
		t.Fatal("restoreDisk succeeded although VDI.create failed")
	}
	if f.lookups != 0 || len(f.destroyed) != 0 {
		t.Fatalf("searched %d times and destroyed %v after xapi refused the create", f.lookups, f.destroyed)
	}
}

var markerVM = pendingVM{dir: "/vm", meta: xapi.VMMeta{VM: map[string]any{"name_label": "web"}}}

func TestUnansweredVMCreateIsDestroyed(t *testing.T) {
	f := &restoreXAPI{dropVM: true}
	p := restoreExporter(t, f)

	if err := p.createVM(context.Background(), markerVM, nil, true); err == nil {
		t.Fatal("createVM succeeded although VM.create never answered")
	}
	// OpaqueRef:samename shares the name but not the marker: it is not ours.
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vm1"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vm1 only", f.destroyed)
	}
}

func TestRestoredVMLosesItsMarker(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)

	if err := p.createVM(context.Background(), markerVM, nil, true); err != nil {
		t.Fatal(err)
	}
	if len(f.unmarked) != 1 || f.unmarked[0] != "OpaqueRef:vm1 "+restoreMarkerKey {
		t.Fatalf("other_config removals %v, want the marker of OpaqueRef:vm1", f.unmarked)
	}
}

func TestNeverSentCallIsNotLookedFor(t *testing.T) {
	// A port nobody listens on: the request cannot leave. A closed httptest server
	// would not do, since the client may reuse a kept-alive connection and read EOF.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, err = http.Post("http://"+addr+"/jsonrpc", "application/json", strings.NewReader("{}"))
	if err == nil {
		t.Fatal("POST to a closed port succeeded")
	}
	if !neverSent(fmt.Errorf("VM.create: %w", err)) {
		t.Fatalf("neverSent(%v) = false for a refused connection", err)
	}
	if neverSent(fmt.Errorf("VM.create: %w", io.EOF)) {
		t.Fatal("neverSent is true for a connection that broke after the request")
	}
	if neverSent(&xapi.Error{Code: "INTERNAL_ERROR"}) {
		t.Fatal("neverSent is true for a xapi error")
	}
}

func TestInterruptBeforeVMsDestroysTheirDisks(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	vm := sizesVM
	vm.record = &connectors.Record{Pathname: "/vm/vm.json"}
	results := make(chan *connectors.Result, 1)
	p.createVMs(ctx, []pendingVM{vm}, orphansDisks, true, results)
	if res := <-results; res.Err == nil {
		t.Fatal("VM reported restored after the interrupt")
	}
	wantDisksDestroyed(t, f)
}

func TestRestoredDiskKeepsItsFileName(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)

	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err != nil {
		t.Fatal(err)
	}
	// A disk restored without its vm.json is never attached, so never renamed there.
	if l := f.vdiLabels["OpaqueRef:vdi0"]; l != "disk-0" {
		t.Fatalf("restored VDI labelled %q, want its file name disk-0", l)
	}
}

var sbVM = pendingVM{dir: "/vm", meta: xapi.VMMeta{
	VM:    map[string]any{"name_label": "web"},
	Disks: []xapi.DiskMeta{{File: "disk-0", VDIUUID: "a", VirtualSize: 4096}},
}}

var sbDisks = map[string]restoredDisk{"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096}}

func TestSecureBootCheckFailureDestroysVMAndDisks(t *testing.T) {
	f := &restoreXAPI{dropSB: true}
	p := restoreExporter(t, f)

	if err := p.createVM(context.Background(), sbVM, sbDisks, true); err == nil {
		t.Fatal("createVM succeeded although the Secure Boot check got no answer")
	}
	// The VM goes first: its VBDs hold the disks.
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vm1", "OpaqueRef:a"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vm1 then OpaqueRef:a", f.destroyed)
	}
}

func TestSecureBootHoldKeepsVM(t *testing.T) {
	f := &restoreXAPI{sbReadiness: "first_boot"}
	p := restoreExporter(t, f)

	err := p.createVM(context.Background(), sbVM, sbDisks, true)
	if err == nil || !strings.Contains(err.Error(), "restored but not started") {
		t.Fatalf("createVM = %v, want the Secure Boot hold", err)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v: a VM held back for Secure Boot is restored, not failed", f.destroyed)
	}
}

func TestDiskWithoutVMIsDestroyed(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	p.opts.srUUID = "sr-uuid"

	records := make(chan *connectors.Record, 1)
	rec := diskRecord(make([]byte, 4096))
	rec.Pathname = "/vm/" + xapi.DiskFilename("0", "a")
	rec.FileInfo.Lmode = 0o644
	records <- rec
	close(records)
	results := make(chan *connectors.Result, 4)
	if err := p.Export(context.Background(), records, results); err != nil {
		t.Fatal(err)
	}
	var got []*connectors.Result
	for r := range results {
		got = append(got, r)
	}
	if len(got) != 1 || got[0].Err == nil || !strings.Contains(got[0].Err.Error(), "vm.json") {
		t.Fatalf("results %v, want one error naming the missing vm.json", got)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi0", f.destroyed)
	}
}
