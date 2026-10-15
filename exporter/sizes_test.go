package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"integration-xcpng/internal/xapi"
)

// destroyRecorder accepts login, VDI.destroy and VDI.resize, and records which VDIs
// were destroyed and which were resized, as "ref size". VDI.resize fails if failResize.
type destroyRecorder struct {
	mu         sync.Mutex
	destroyed  []string
	resized    []string
	failResize bool
}

func (f *destroyRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var result any
	switch req.Method {
	case "session.login_with_password":
		result = "OpaqueRef:session"
	case "VDI.destroy":
		var ref string
		_ = json.Unmarshal(req.Params[1], &ref)
		f.mu.Lock()
		f.destroyed = append(f.destroyed, ref)
		f.mu.Unlock()
		result = ""
	case "VDI.resize":
		if f.failResize {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": 1, "message": "SR_BACKEND_FAILURE", "data": []string{}},
			})
			return
		}
		var ref string
		_ = json.Unmarshal(req.Params[1], &ref)
		f.mu.Lock()
		f.resized = append(f.resized, ref+" "+string(req.Params[2]))
		f.mu.Unlock()
		result = ""
	default:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": "MESSAGE_METHOD_UNKNOWN", "data": []string{req.Method}},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func sizesExporter(t *testing.T, f *destroyRecorder, opts options) *Exporter {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	c, err := xapi.Dial(context.Background(), &xapi.Config{
		Host:        strings.TrimPrefix(srv.URL, "https://"),
		TLSInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Exporter{client: c, opts: opts}
}

var sizesVM = pendingVM{dir: "/vm", meta: xapi.VMMeta{Disks: []xapi.DiskMeta{
	{File: "disk-0", VDIUUID: "a", VirtualSize: 4096},
	{File: "disk-1", VDIUUID: "b", VirtualSize: 8192},
}}}

func TestCheckDiskSizesRefusesShortDisk(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{})
	vm := sizesVM

	full := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 8192, kept: 8192},
	}
	if err := p.checkDiskSizes(context.Background(), vm, full, true); err != nil {
		t.Fatalf("complete disks refused: %v", err)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v on a complete restore", f.destroyed)
	}

	short := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 1000, kept: 1000},
	}
	err := p.checkDiskSizes(context.Background(), vm, short, true)
	if err == nil || !strings.Contains(err.Error(), "disk-1") {
		t.Fatalf("short disk not refused: %v", err)
	}
	// Both disks go: the VM is not created, so the complete one would have no owner.
	if len(f.destroyed) != 2 || f.destroyed[0] != "OpaqueRef:a" || f.destroyed[1] != "OpaqueRef:b" {
		t.Fatalf("destroyed %v, want OpaqueRef:a and OpaqueRef:b", f.destroyed)
	}
	if len(f.resized) != 0 {
		t.Fatalf("resized %v without allow_incomplete_disks", f.resized)
	}
}

func TestCheckDiskSizesAllowIncompleteGrowsShortDisk(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{allowShort: true})

	short := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 1000, kept: 1000},
	}
	if err := p.checkDiskSizes(context.Background(), sizesVM, short, true); err != nil {
		t.Fatalf("short disk refused with allow_incomplete_disks: %v", err)
	}
	if len(f.resized) != 1 || f.resized[0] != "OpaqueRef:b 8192" {
		t.Fatalf("resized %v, want OpaqueRef:b to 8192", f.resized)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v on a rescued restore", f.destroyed)
	}
}

func TestCheckDiskSizesAllowIncompleteStillRefuses(t *testing.T) {
	cases := []struct {
		name       string
		size       int64
		zeroed     bool
		failResize bool
		want       string
	}{
		// The grown end would not read as zeroes.
		{name: "SR does not zero new VDIs", size: 1000, zeroed: false, want: "needs an SR"},
		// Growing cannot explain a disk larger than recorded.
		{name: "disk larger than recorded", size: 9000, zeroed: true, want: "incomplete"},
		{name: "resize fails", size: 1000, zeroed: true, failResize: true, want: "growing disk disk-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &destroyRecorder{failResize: tc.failResize}
			p := sizesExporter(t, f, options{allowShort: true})
			restored := map[string]restoredDisk{
				"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
				"/vm/b": {ref: "OpaqueRef:b", size: tc.size, kept: tc.size},
			}
			err := p.checkDiskSizes(context.Background(), sizesVM, restored, tc.zeroed)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
			if !tc.failResize && len(f.resized) != 0 {
				t.Fatalf("resized %v", f.resized)
			}
			if len(f.destroyed) != 2 {
				t.Fatalf("destroyed %v, want both disks", f.destroyed)
			}
		})
	}
}
