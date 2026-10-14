package xapi

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
	"time"
)

type chunk struct {
	off int64
	n   int
}

// received is what the fake import handler saw.
type received struct {
	chunks []chunk
	err    error // set when the stream ended without its end marker
	done   chan struct{}
}

// chunkedServer stands in for xapi's import_raw_vdi handler: it answers before
// reading the body, as xapi does, then applies the chunks to disk.
func chunkedServer(t *testing.T, disk []byte, status int, got *received) *httptest.Server {
	t.Helper()
	got.done = make(chan struct{})
	srv := httptest.NewTLSServer(withTasks(t, &fakeTasks{status: "success"}, func(w http.ResponseWriter, r *http.Request) {
		defer close(got.done)
		q := r.URL.Query()
		if r.Method != http.MethodPut || r.URL.Path != "/import_raw_vdi" || q.Get("chunked") != "true" || q.Get("format") != "raw" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if len(r.TransferEncoding) != 0 || r.Header.Get("Content-Length") != "" {
			t.Errorf("request has a length or transfer encoding: %v %q", r.TransferEncoding, r.Header.Get("Content-Length"))
		}
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 %d %s\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
		_ = rw.Flush()
		if status != http.StatusOK {
			_, _ = io.Copy(io.Discard, rw)
			return
		}
		got.chunks, got.err = readChunks(rw.Reader, disk)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeTasks stands in for xapi's task API: one task, which ends with status and,
// on failure, errorInfo. The import handler must name it in task_id.
type fakeTasks struct {
	status    string
	errorInfo []string

	mu        sync.Mutex
	created   int
	destroyed int
	importArg string
}

const fakeTaskRef = "OpaqueRef:task"

// withTasks serves the task calls on /jsonrpc and passes the rest to h.
func withTasks(t *testing.T, ft *fakeTasks, h http.HandlerFunc) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jsonrpc" {
			ft.mu.Lock()
			ft.importArg = r.URL.Query().Get("task_id")
			ft.mu.Unlock()
			h(w, r)
			return
		}
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding rpc: %v", err)
			return
		}
		ft.mu.Lock()
		defer ft.mu.Unlock()
		var result any
		switch req.Method {
		case "task.create":
			ft.created++
			result = fakeTaskRef
		case "task.get_status":
			result = ft.status
		case "task.get_error_info":
			result = ft.errorInfo
		case "task.destroy":
			ft.destroyed++
			result = ""
		default:
			t.Errorf("unexpected rpc %s", req.Method)
		}
		if len(req.Params) > 1 && req.Method != "task.create" && req.Params[1] != fakeTaskRef {
			t.Errorf("%s on %v, want %s", req.Method, req.Params[1], fakeTaskRef)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
	})
}

// check reports a task that was not passed to the handler or not destroyed.
func (ft *fakeTasks) check(t *testing.T) {
	t.Helper()
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.importArg != fakeTaskRef {
		t.Errorf("import task_id = %q, want %q", ft.importArg, fakeTaskRef)
	}
	if ft.created != 1 || ft.destroyed != 1 {
		t.Errorf("tasks created %d, destroyed %d; want 1 and 1", ft.created, ft.destroyed)
	}
}

func readChunks(r *bufio.Reader, disk []byte) ([]chunk, error) {
	var chunks []chunk
	var hdr [12]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return chunks, fmt.Errorf("reading chunk header: %w", err)
		}
		off := int64(binary.LittleEndian.Uint64(hdr[:8]))
		n := int(binary.LittleEndian.Uint32(hdr[8:]))
		if off == 0 && n == 0 {
			return chunks, nil
		}
		if _, err := io.ReadFull(r, disk[off:off+int64(n)]); err != nil {
			return chunks, fmt.Errorf("reading chunk at %d: %w", off, err)
		}
		chunks = append(chunks, chunk{off, n})
	}
}

func testClient(srv *httptest.Server) *Client {
	tr := srv.Client().Transport.(*http.Transport)
	return &Client{base: srv.URL, http: srv.Client(), tls: tr.TLSClientConfig, session: "OpaqueRef:s"}
}

func TestImportSparseVDISkipsZeroBlocks(t *testing.T) {
	const tail = 1000
	size := int64(5*sparseBlock + tail)
	src := make([]byte, size)
	for i := range sparseBlock {
		src[1*sparseBlock+i] = byte(i%251 + 1)
	}
	src[3*sparseBlock-1] = 7 // last byte of block 2
	for i := range sparseBlock {
		src[4*sparseBlock+i] = 0xff
	}
	for i := range tail {
		src[5*sparseBlock+i] = 0x55
	}
	// blocks 0 and 3 stay zero

	disk := make([]byte, size)
	var got received
	c := testClient(chunkedServer(t, disk, http.StatusOK, &got))

	sent, err := c.ImportSparseVDI(context.Background(), "OpaqueRef:vdi", size, bytes.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	<-got.done
	if got.err != nil {
		t.Fatal(got.err)
	}
	chunks := got.chunks
	if !bytes.Equal(disk, src) {
		t.Fatal("restored disk differs from the source")
	}
	want := []chunk{{1 * sparseBlock, sparseBlock}, {2 * sparseBlock, sparseBlock}, {4 * sparseBlock, sparseBlock}, {5 * sparseBlock, tail}}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %v, want %v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunks = %v, want %v", chunks, want)
		}
	}
	if sent != int64(3*sparseBlock+tail) {
		t.Fatalf("sent = %d, want %d", sent, 3*sparseBlock+tail)
	}
}

func TestImportSparseVDIReportsStatus(t *testing.T) {
	var got received
	c := testClient(chunkedServer(t, nil, http.StatusNotFound, &got))
	src := bytes.Repeat([]byte{1}, 4096)
	_, err := c.ImportSparseVDI(context.Background(), "OpaqueRef:vdi", int64(len(src)), bytes.NewReader(src))
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v, want an HTTP 404 error", err)
	}
}

func TestImportSparseVDIShortSource(t *testing.T) {
	disk := make([]byte, 2*sparseBlock)
	var got received
	c := testClient(chunkedServer(t, disk, http.StatusOK, &got))
	_, err := c.ImportSparseVDI(context.Background(), "OpaqueRef:vdi", int64(len(disk)), bytes.NewReader([]byte{1, 2, 3}))
	if err == nil {
		t.Fatal("a source shorter than the disk did not fail")
	}
	<-got.done
	if got.err == nil {
		t.Fatal("the stream was ended cleanly; xapi would have accepted a truncated disk")
	}
}

func TestSRZeroesNewVDIs(t *testing.T) {
	for _, tc := range []struct {
		typ  string
		sm   map[string]string
		want bool
	}{
		{"ext", nil, true},
		{"nfs", nil, true},
		{"lvm", nil, false},
		{"lvmoiscsi", map[string]string{"use_vhd": "true"}, true},
		{"LVMoHBA", map[string]string{}, false},
		{"equal", map[string]string{"Allocation": "Thick"}, false},
		{"equal", nil, true},
		{"cslg", nil, false},
	} {
		if got := SRZeroesNewVDIs(tc.typ, tc.sm); got != tc.want {
			t.Errorf("SRZeroesNewVDIs(%q, %v) = %v, want %v", tc.typ, tc.sm, got, tc.want)
		}
	}
}

// afterAccept is what xapi was seen sending after its 200 when the write failed: it
// varies from one run to the next on the same restore.
var afterAccept = map[string]string{
	"nothing":   "",
	"400":       "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nCache-Control: no-cache, no-store\r\n\r\n",
	"400 + 500": "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\nHTTP/1.1 500 Internal Error\r\ncontent-length: 12\r\nconnection: close\r\n\r\nVDI_IO_ERROR",
}

// TestImportSparseVDIWriteFailure covers xapi failing the write after it has
// answered 200. Only the task says so reliably.
func TestImportSparseVDIWriteFailure(t *testing.T) {
	for name, after := range afterAccept {
		t.Run(name, func(t *testing.T) {
			disk := make([]byte, 2*sparseBlock)
			ft := &fakeTasks{status: "failure", errorInfo: []string{"VDI_IO_ERROR", "Device I/O errors"}}
			srv := httptest.NewTLSServer(withTasks(t, ft, func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				defer func() { _ = conn.Close() }()
				_, _ = fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
				_ = rw.Flush()
				if _, err := readChunks(rw.Reader, disk); err != nil {
					t.Errorf("reading chunks: %v", err)
				}
				_, _ = fmt.Fprint(rw, after)
				_ = rw.Flush()
			}))
			t.Cleanup(srv.Close)
			c := testClient(srv)

			src := bytes.Repeat([]byte{1}, len(disk))
			_, err := c.ImportSparseVDI(context.Background(), "OpaqueRef:vdi", int64(len(src)), bytes.NewReader(src))
			if !IsCode(err, "VDI_IO_ERROR") {
				t.Fatalf("err = %v, want VDI_IO_ERROR", err)
			}
			ft.check(t)
		})
	}
}

// TestImportRawVDIWriteFailure is TestImportSparseVDIWriteFailure for the plain import.
func TestImportRawVDIWriteFailure(t *testing.T) {
	for _, status := range []string{"success", "failure"} {
		t.Run(status, func(t *testing.T) {
			ft := &fakeTasks{status: status, errorInfo: []string{"VDI_IO_ERROR", "Device I/O errors"}}
			srv := httptest.NewTLSServer(withTasks(t, ft, func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				defer func() { _ = conn.Close() }()
				_, _ = fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
				_ = rw.Flush()
				if _, err := io.CopyN(io.Discard, rw, r.ContentLength); err != nil {
					t.Errorf("reading body: %v", err)
				}
			}))
			t.Cleanup(srv.Close)
			c := testClient(srv)
			src := bytes.Repeat([]byte{1}, 1<<20)
			err := c.ImportRawVDI(context.Background(), "OpaqueRef:vdi", int64(len(src)), bytes.NewReader(src))
			if status == "failure" && !IsCode(err, "VDI_IO_ERROR") {
				t.Fatalf("err = %v, want VDI_IO_ERROR", err)
			}
			if status == "success" && err != nil {
				t.Fatal(err)
			}
			ft.check(t)
		})
	}
}

// TestImportResultWaitsForPendingTask covers a task read before xapi has ended it.
func TestImportResultWaitsForPendingTask(t *testing.T) {
	ft := &fakeTasks{status: "pending"}
	srv := httptest.NewTLSServer(withTasks(t, ft, nil))
	t.Cleanup(srv.Close)
	c := testClient(srv)
	go func() {
		time.Sleep(300 * time.Millisecond)
		ft.mu.Lock()
		ft.status = "success"
		ft.mu.Unlock()
	}()
	if err := c.importResult(context.Background(), fakeTaskRef); err != nil {
		t.Fatal(err)
	}

	ft.mu.Lock()
	ft.status = "pending"
	ft.mu.Unlock()
	defer func(d time.Duration) { importTaskWait = d }(importTaskWait)
	importTaskWait = 100 * time.Millisecond
	if err := c.importResult(context.Background(), fakeTaskRef); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("err = %v, want a still-pending error", err)
	}
}
