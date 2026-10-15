package exporter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"integration-xcpng/internal/xapi"
)

// destroyRecorder accepts login, VDI.destroy, VDI.resize, the task calls of an import
// and chunked imports, and records which VDIs were destroyed, which were resized, as
// "ref size", and the chunks written, as "ref offset" and the data. VDI.resize fails if
// failResize.
type destroyRecorder struct {
	mu         sync.Mutex
	destroyed  []string
	resized    []string
	written    map[string][]byte
	failResize bool
}

// recordImport answers a chunked import_raw_vdi with 200 and records its chunks.
func (f *destroyRecorder) recordImport(w http.ResponseWriter, r *http.Request) {
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_, _ = fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
	_ = rw.Flush()
	ref := r.URL.Query().Get("vdi")
	br := bufio.NewReader(rw)
	var hdr [12]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return
		}
		off := binary.LittleEndian.Uint64(hdr[:8])
		n := binary.LittleEndian.Uint32(hdr[8:])
		if off == 0 && n == 0 {
			return
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(br, data); err != nil {
			return
		}
		f.mu.Lock()
		if f.written == nil {
			f.written = map[string][]byte{}
		}
		f.written[fmt.Sprintf("%s %d", ref, off)] = data
		f.mu.Unlock()
	}
}

func (f *destroyRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/import_raw_vdi" {
		f.recordImport(w, r)
		return
	}
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
	case "task.create":
		result = "OpaqueRef:task"
	case "task.get_status":
		result = "success"
	case "task.destroy":
		result = ""
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

// A complete disk whose end looked like xapi's error page had that end zeroed by
// shortTail. Once vm.json shows the disk is complete, those bytes go back, unchanged.
func TestCheckDiskSizesPutsBackTailOfCompleteDisk(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{allowShort: true})
	cut := bytes.Repeat([]byte{7}, 1024)
	restored := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 8192, stored: 8192, kept: 7168, cut: cut, dropped: "the stored file ended with a page"},
	}
	if err := p.checkDiskSizes(context.Background(), sizesVM, restored, true); err != nil {
		t.Fatalf("complete disk refused: %v", err)
	}
	if got := f.written["OpaqueRef:b 7168"]; len(f.written) != 1 || !bytes.Equal(got, cut) {
		t.Fatalf("written %d chunk(s), want the 1024 cut bytes back at 7168 of OpaqueRef:b", len(f.written))
	}
	if len(f.resized) != 0 || len(f.destroyed) != 0 {
		t.Fatalf("resized %v, destroyed %v on a complete disk", f.resized, f.destroyed)
	}
}

// A short disk keeps the page removed: nothing is written back.
func TestCheckDiskSizesKeepsPageOffShortDisk(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{allowShort: true})
	restored := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 4096, stored: 4096, kept: 3584, cut: bytes.Repeat([]byte{7}, 512), dropped: "page"},
	}
	if err := p.checkDiskSizes(context.Background(), sizesVM, restored, true); err != nil {
		t.Fatalf("short disk refused with allow_incomplete_disks: %v", err)
	}
	if len(f.written) != 0 {
		t.Fatalf("wrote %d chunk(s) back into a short disk", len(f.written))
	}
	if len(f.resized) != 1 || f.resized[0] != "OpaqueRef:b 8192" {
		t.Fatalf("resized %v, want OpaqueRef:b to 8192", f.resized)
	}
}
