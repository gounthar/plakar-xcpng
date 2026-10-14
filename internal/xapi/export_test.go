package xapi

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// exportServer answers like export_raw_vdi: 200, Connection: close, no
// Content-Length, then sends body and closes the connection, with a TLS
// close_notify or, with abrupt set, by dropping the TCP connection.
func exportServer(t *testing.T, body []byte, abrupt bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/octet-stream\r\n\r\n")
		_, _ = rw.Write(body)
		_ = rw.Flush()
		if nc, ok := conn.(interface{ NetConn() net.Conn }); abrupt && ok {
			_ = nc.NetConn().Close()
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestExportRawVDIChecksSize(t *testing.T) {
	const size = 3000
	for _, tc := range []struct {
		name   string
		sent   int
		abrupt bool
		short  bool // want io.ErrUnexpectedEOF
		fail   bool
	}{
		{name: "exact", sent: size},
		{name: "cut off", sent: 1000, short: true, fail: true},
		{name: "cut off without close_notify", sent: 1000, abrupt: true, short: true, fail: true},
		{name: "empty", sent: 0, short: true, fail: true},
		{name: "too long", sent: size + 1, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(exportServer(t, make([]byte, tc.sent), tc.abrupt))
			rc, err := c.ExportRawVDI(context.Background(), "OpaqueRef:vdi", size)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rc.Close() }()
			n, err := io.Copy(io.Discard, rc)
			if tc.fail != (err != nil) {
				t.Fatalf("read %d bytes, err %v; want failure %v", n, err, tc.fail)
			}
			if tc.short && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("err %v does not wrap io.ErrUnexpectedEOF", err)
			}
		})
	}
}
