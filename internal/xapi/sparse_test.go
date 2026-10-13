package xapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
