package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"integration-xcpng/internal/watchdog"
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
	mu           sync.Mutex
	dropVDI      bool
	refuseVDI    bool
	dropVM       bool
	busyLooks    int
	vdiLabels    map[string]string // ref -> name_label
	vmMarkers    map[string]string // ref -> other_config marker
	vdiDescs     []string          // name_description of each VDI.create
	vdiDesc      map[string]string // ref -> current name_description
	vbds         map[string]string // VDI ref -> VM ref, from VBD.create
	lostID       []string          // VDIs renamed out of the restore ID while no VBD held them
	destroyed    []string          // VDIs and VMs, in order
	unmarked     []string
	unmarkedAtSB int // len(unmarked) when the Secure Boot checks began
	lookups      int // VDI.get_by_name_label and VM.get_by_name_label calls
	srOpsReads   int
	// dropSB drops VM.get_secureboot_certificates_state. sbReadiness, when set, is the
	// VM's Secure Boot readiness, on a pool whose own readiness is not_ready.
	dropSB      bool
	dropSBOnce  bool // drops only the first VM.get_secureboot_certificates_state
	sbReadiness string
	// sessionLossOnce fails the first SR.get_current_operations with SESSION_INVALID,
	// as after a toolstack restart; hangOnce never answers the first one. refuseVM
	// fails VM.create with a xapi error.
	sessionLossOnce bool
	hangOnce        bool
	refuseVM        bool
	logins          int
	// inUse answers that many VDI.destroy calls with VDI_IN_USE, as xapi does for a
	// moment after an import's connection is cut.
	inUse int
	// held answers every VDI.destroy of these refs with VDI_IN_USE.
	held map[string]bool
	// heldFor answers that many VDI.destroy calls of a ref with VDI_IN_USE, then lets
	// it go.
	heldFor map[string]int
	// dropDestroy drops every VDI.destroy, as a host that stopped answering does;
	// afterHeld does so once heldFor has run out, instead of destroying.
	dropDestroy bool
	afterHeld   bool
}

// checkID records ref when a rename leaves it with no restore ID in its name or
// description while no VBD holds it: killed then, with the watchdog lost, nothing
// leads to it. Called with f.mu held.
func (f *restoreXAPI) checkID(ref string) {
	if _, ok := f.vbds[ref]; ok {
		return
	}
	if !strings.Contains(f.vdiLabels[ref], "plakar-restore-") && !strings.Contains(f.vdiDesc[ref], "plakar-restore-") {
		f.lostID = append(f.lostID, ref)
	}
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
	if req.Method == "SR.get_current_operations" {
		f.mu.Lock()
		hang := f.hangOnce
		f.hangOnce = false
		f.mu.Unlock()
		if hang {
			<-r.Context().Done()
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var result any = ""
	switch req.Method {
	case "session.login_with_password":
		f.logins++
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
			NameLabel       string `json:"name_label"`
			NameDescription string `json:"name_description"`
		}
		_ = json.Unmarshal(req.Params[1], &rec)
		f.vdiDescs = append(f.vdiDescs, rec.NameDescription)
		ref := fmt.Sprintf("OpaqueRef:vdi%d", len(f.vdiLabels))
		if f.vdiLabels == nil {
			f.vdiLabels = map[string]string{}
			f.vdiDesc = map[string]string{}
		}
		f.vdiLabels[ref] = rec.NameLabel
		f.vdiDesc[ref] = rec.NameDescription
		if f.dropVDI {
			drop()
			return
		}
		result = ref
	case "SR.get_current_operations":
		if f.sessionLossOnce {
			f.sessionLossOnce = false
			xapiErr("SESSION_INVALID")
			return
		}
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
		if req.Method == "VDI.destroy" && f.dropDestroy {
			drop()
			return
		}
		if req.Method == "VDI.destroy" && f.held[str(1)] {
			xapiErr("VDI_IN_USE")
			return
		}
		if req.Method == "VDI.destroy" && f.heldFor[str(1)] > 0 {
			f.heldFor[str(1)]--
			xapiErr("VDI_IN_USE")
			return
		}
		if req.Method == "VDI.destroy" && f.afterHeld {
			drop()
			return
		}
		if req.Method == "VDI.destroy" && f.inUse > 0 {
			f.inUse--
			xapiErr("VDI_IN_USE")
			return
		}
		f.destroyed = append(f.destroyed, str(1))
	case "VDI.set_name_label":
		if f.vdiLabels == nil {
			f.vdiLabels = map[string]string{}
		}
		f.vdiLabels[str(1)] = str(2)
		f.checkID(str(1))
	case "VDI.set_name_description":
		if f.vdiDesc == nil {
			f.vdiDesc = map[string]string{}
		}
		f.vdiDesc[str(1)] = str(2)
		f.checkID(str(1))
	case "VBD.create":
		var rec struct {
			VM  string `json:"VM"`
			VDI string `json:"VDI"`
		}
		_ = json.Unmarshal(req.Params[1], &rec)
		if f.vbds == nil {
			f.vbds = map[string]string{}
		}
		f.vbds[rec.VDI] = rec.VM
	case "VM.create":
		if f.refuseVM {
			xapiErr("INTERNAL_ERROR")
			return
		}
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
		f.unmarkedAtSB = len(f.unmarked)
		if f.dropSB || f.dropSBOnce {
			f.dropSBOnce = false
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
	return &Exporter{client: c, watchdog: watchdog.FromWriter(&recordWatchdog{})}
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
	// The marker comes off only once the VM is kept, held or not: during the Secure
	// Boot checks a killed restore with no watchdog leaves a VM only the marker names.
	for _, readiness := range []string{"", "first_boot"} {
		f := &restoreXAPI{sbReadiness: readiness}
		p := restoreExporter(t, f)

		err := p.createVM(context.Background(), markerVM, nil, true)
		var held *heldError
		if err != nil && !errors.As(err, &held) {
			t.Fatalf("readiness %q: %v", readiness, err)
		}
		if len(f.unmarked) != 1 || f.unmarked[0] != "OpaqueRef:vm1 "+restoreMarkerKey {
			t.Fatalf("readiness %q: other_config removals %v, want the marker of OpaqueRef:vm1", readiness, f.unmarked)
		}
		if f.unmarkedAtSB != 0 {
			t.Fatalf("readiness %q: marker removed before the Secure Boot checks", readiness)
		}
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
	old := cleanupRetry
	// It also bounds the destroy that follows, which opens a new TLS connection after the
	// dropped one: under riscv64 emulation that takes longer than 50 ms.
	cleanupRetry = time.Second
	t.Cleanup(func() { cleanupRetry = old })
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

func TestVDILookupSurvivesXAPIRestart(t *testing.T) {
	f := &restoreXAPI{dropVDI: true, sessionLossOnce: true}
	p := restoreExporter(t, f)

	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err == nil {
		t.Fatal("restoreDisk succeeded although VDI.create never answered")
	}
	if f.logins < 2 {
		t.Fatalf("%d logins: the lost session was not renewed", f.logins)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi0", f.destroyed)
	}
}

func TestVDILookupOutlivesAHungCall(t *testing.T) {
	old := attemptTimeout
	attemptTimeout = 50 * time.Millisecond
	t.Cleanup(func() { attemptTimeout = old })
	f := &restoreXAPI{dropVDI: true, hangOnce: true}
	p := restoreExporter(t, f)

	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err == nil {
		t.Fatal("restoreDisk succeeded although VDI.create never answered")
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi0", f.destroyed)
	}
}

func TestBusySRStillLooksForTheVDI(t *testing.T) {
	old := restoreWait
	restoreWait = 30 * time.Millisecond
	t.Cleanup(func() { restoreWait = old })
	f := &restoreXAPI{dropVDI: true, busyLooks: 1 << 30} // another client's vdi_create never ends
	p := restoreExporter(t, f)

	_, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096)))
	if err == nil || strings.Contains(err.Error(), "left behind") {
		t.Fatalf("restoreDisk = %v, want the create error alone: the VDI was found and destroyed", err)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi0", f.destroyed)
	}
}

// exportRecords runs Export on the given records and returns the results by pathname.
func exportRecords(t *testing.T, p *Exporter, recs ...*connectors.Record) map[string]error {
	t.Helper()
	records := make(chan *connectors.Record, len(recs))
	for _, r := range recs {
		records <- r
	}
	close(records)
	results := make(chan *connectors.Result, len(recs)+1)
	if err := p.Export(context.Background(), records, results); err != nil {
		t.Fatal(err)
	}
	got := map[string]error{}
	for r := range results {
		got[r.Record.Pathname] = r.Err
	}
	return got
}

func metaRecord(t *testing.T, dir string, raw []byte) *connectors.Record {
	t.Helper()
	return &connectors.Record{
		Pathname: dir + "/" + xapi.MetaFilename,
		FileInfo: objects.FileInfo{Lsize: int64(len(raw)), Lmode: 0o644},
		Reader:   io.NopCloser(bytes.NewReader(raw)),
	}
}

func vmJSON(t *testing.T, uuids ...string) []byte {
	t.Helper()
	meta := xapi.VMMeta{Version: xapi.MetaVersion, VM: map[string]any{"name_label": "web"}}
	for _, u := range uuids {
		meta.Disks = append(meta.Disks, xapi.DiskMeta{File: xapi.DiskFilename("0", u), VDIUUID: u, VirtualSize: 4096, Userdevice: "0"})
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func diskIn(dir, uuid string) *connectors.Record {
	r := diskRecord(make([]byte, 4096))
	r.Pathname = dir + "/" + xapi.DiskFilename("0", uuid)
	r.FileInfo.Lmode = 0o644
	return r
}

func TestDiskNotInVMJSONIsDestroyed(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	p.opts.srUUID = "sr-uuid"

	got := exportRecords(t, p, diskIn("/vm", "a"), diskIn("/vm", "extra"), metaRecord(t, "/vm", vmJSON(t, "a")))
	if err := got["/vm/"+xapi.DiskFilename("0", "extra")]; err == nil {
		t.Fatal("a disk vm.json does not list was reported restored")
	}
	if err := got["/vm/"+xapi.DiskFilename("0", "a")]; err != nil {
		t.Fatalf("the VM's own disk failed: %v", err)
	}
	if !slices.Contains(f.destroyed, "OpaqueRef:vdi1") || slices.Contains(f.destroyed, "OpaqueRef:vdi0") {
		t.Fatalf("destroyed %v, want OpaqueRef:vdi1 (the extra disk) only", f.destroyed)
	}
}

func TestDisksOfAFailedVMAreReportedFailed(t *testing.T) {
	f := &restoreXAPI{refuseVM: true}
	p := restoreExporter(t, f)
	p.opts.srUUID = "sr-uuid"

	got := exportRecords(t, p, diskIn("/vm", "a"), metaRecord(t, "/vm", vmJSON(t, "a")))
	if err := got["/vm/"+xapi.DiskFilename("0", "a")]; err == nil {
		t.Fatal("the disk of a VM that was not restored was reported restored")
	}
}

func TestDiskBesideUnreadableVMJSONSaysSo(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	p.opts.srUUID = "sr-uuid"

	got := exportRecords(t, p, diskIn("/vm", "a"), metaRecord(t, "/vm", []byte("{not json")))
	err := got["/vm/"+xapi.DiskFilename("0", "a")]
	if err == nil || strings.Contains(err.Error(), "no vm.json") {
		t.Fatalf("disk result %v, want an error naming the unreadable vm.json", err)
	}
}

func TestSecureBootCheckSurvivesABlip(t *testing.T) {
	f := &restoreXAPI{dropSBOnce: true}
	p := restoreExporter(t, f)

	if err := p.createVM(context.Background(), sbVM, sbDisks, true); err != nil {
		t.Fatalf("createVM = %v: one dropped read failed a complete restore", err)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v after one dropped read", f.destroyed)
	}
}

func TestFailedStartKeepsVMAndReportsDisksRestored(t *testing.T) {
	f := &restoreXAPI{} // VM.start is unknown to the fake, so it fails
	p := restoreExporter(t, f)
	p.opts.srUUID = "sr-uuid"
	p.opts.startOnRestore = true

	got := exportRecords(t, p, diskIn("/vm", "a"), metaRecord(t, "/vm", vmJSON(t, "a")))
	if err := got["/vm/"+xapi.MetaFilename]; err == nil || !strings.Contains(err.Error(), "not started") {
		t.Fatalf("vm.json result %v, want the start failure", err)
	}
	if err := got["/vm/"+xapi.DiskFilename("0", "a")]; err != nil {
		t.Fatalf("disk result %v: the disk is kept with its VM", err)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v: a VM that only failed to start is kept", f.destroyed)
	}
}
