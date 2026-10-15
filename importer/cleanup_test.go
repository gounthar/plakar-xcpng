package importer

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"integration-xcpng/internal/xapi"
)

// snapshotXAPI answers the calls destroySnapshot makes, for one snapshot VM with two
// snapshot disks, and records every destroy call in order.
type snapshotXAPI struct {
	failVDI string // VDI ref whose VDI.destroy fails
	// down is how many requests are dropped without an answer, as during a toolstack
	// restart. Once one is dropped, the old session is invalid, as after a restart.
	down int

	mu        sync.Mutex
	calls     []string // "VDI.destroy <ref>", "VM.destroy <ref>"
	restarted bool
}

func (f *snapshotXAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	if f.down > 0 {
		f.down--
		f.restarted = true
		f.mu.Unlock()
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
		return
	}
	session := "OpaqueRef:session"
	if f.restarted {
		session = "OpaqueRef:session-after-restart"
	}
	f.mu.Unlock()
	arg := func(i int) string {
		var s string
		if i < len(req.Params) {
			_ = json.Unmarshal(req.Params[i], &s)
		}
		return s
	}
	fail := func(code string) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": code, "data": []string{arg(1)}},
		})
	}
	if req.Method != "session.login_with_password" && arg(0) != session {
		fail("SESSION_INVALID")
		return
	}
	var result any
	switch req.Method {
	case "session.login_with_password":
		result = session
	case "VM.get_VBDs":
		result = []string{"OpaqueRef:vbd-a", "OpaqueRef:vbd-b", "OpaqueRef:vbd-cd"}
	case "VBD.get_record":
		switch arg(1) {
		case "OpaqueRef:vbd-a":
			result = xapi.VBDRecord{Type: "Disk", VDI: "OpaqueRef:vdi-a"}
		case "OpaqueRef:vbd-b":
			result = xapi.VBDRecord{Type: "Disk", VDI: "OpaqueRef:vdi-b"}
		default:
			result = xapi.VBDRecord{Type: "CD", VDI: xapi.NullRef, Empty: true}
		}
	case "VDI.get_is_a_snapshot":
		result = true
	case "VDI.destroy", "VM.destroy":
		f.mu.Lock()
		f.calls = append(f.calls, req.Method+" "+arg(1))
		f.mu.Unlock()
		if req.Method == "VDI.destroy" && arg(1) == f.failVDI {
			fail("SR_BACKEND_FAILURE_1200")
			return
		}
		result = ""
	default:
		fail("MESSAGE_METHOD_UNKNOWN")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func TestDestroySnapshotDestroysVDIsBeforeVM(t *testing.T) {
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// A VDI that cannot be destroyed must leave the snapshot VM in place: it carries the
// marker the sweep looks for, and without it the VDI would be leaked for good.
func TestDestroySnapshotKeepsVMWhenAVDIFails(t *testing.T) {
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	err := p.destroySnapshot(context.Background(), "OpaqueRef:snap")
	if !xapi.IsCode(err, "SR_BACKEND_FAILURE_1200") {
		t.Fatalf("err = %v, want SR_BACKEND_FAILURE_1200", err)
	}
	// The other disk is still destroyed: one bad VDI does not leak the rest.
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q (no VM.destroy)", f.calls, want)
	}
}

// A toolstack restart right after the export used to make cleanup fail at once with
// "connection refused", leaking the snapshot and its VDIs. Cleanup must wait for XAPI
// to come back and log in again, since sessions do not survive a restart.
func TestDestroySnapshotWaitsForXAPIRestart(t *testing.T) {
	fastRetry(t)
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.down = 3
	f.mu.Unlock()
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// The wait for XAPI is bounded by the caller's context.
func TestDestroySnapshotGivesUpWhenXAPIStaysDown(t *testing.T) {
	fastRetry(t)
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.down = 1 << 30
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := p.destroySnapshot(ctx, "OpaqueRef:snap"); err == nil {
		t.Fatal("destroySnapshot succeeded with XAPI down")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("gave up after %s, want about 500ms", d)
	}
}

func fastRetry(t *testing.T) {
	t.Helper()
	old := xapiDownDelay
	xapiDownDelay = 10 * time.Millisecond
	t.Cleanup(func() { xapiDownDelay = old })
}
