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

// destroyRecorder accepts login and VDI.destroy, and records which VDIs were destroyed.
type destroyRecorder struct {
	mu        sync.Mutex
	destroyed []string
}

func (f *destroyRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
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
		f.mu.Lock()
		f.destroyed = append(f.destroyed, req.Params[1])
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

func TestCheckDiskSizesRefusesShortDisk(t *testing.T) {
	f := &destroyRecorder{}
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	c, err := xapi.Dial(context.Background(), &xapi.Config{
		Host:        strings.TrimPrefix(srv.URL, "https://"),
		TLSInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &Exporter{client: c}

	vm := pendingVM{dir: "/vm", meta: xapi.VMMeta{Disks: []xapi.DiskMeta{
		{File: "disk-0", VDIUUID: "a", VirtualSize: 4096},
		{File: "disk-1", VDIUUID: "b", VirtualSize: 8192},
	}}}

	full := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 8192},
	}
	if err := p.checkDiskSizes(context.Background(), vm, full); err != nil {
		t.Fatalf("complete disks refused: %v", err)
	}
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %v on a complete restore", f.destroyed)
	}

	short := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 1000},
	}
	err = p.checkDiskSizes(context.Background(), vm, short)
	if err == nil || !strings.Contains(err.Error(), "disk-1") {
		t.Fatalf("short disk not refused: %v", err)
	}
	// Both disks go: the VM is not created, so the complete one would have no owner.
	if len(f.destroyed) != 2 || f.destroyed[0] != "OpaqueRef:a" || f.destroyed[1] != "OpaqueRef:b" {
		t.Fatalf("destroyed %v, want OpaqueRef:a and OpaqueRef:b", f.destroyed)
	}
}
