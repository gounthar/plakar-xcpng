// Package xapi is a small XAPI client: JSON-RPC calls on /jsonrpc, and the raw VDI
// export and import HTTP handlers.
package xapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Protocol is the location scheme and the connector protocol name.
const Protocol = "xcpng"

// NullRef is XAPI's null reference.
const NullRef = "OpaqueRef:NULL"

// maxResponseSize bounds a JSON-RPC response. The largest calls made here are
// get_all_records on VMs, which stay well below this on any real pool.
const maxResponseSize = 256 << 20

// Error is an error returned by XAPI, such as HANDLE_INVALID or HOST_IS_SLAVE.
type Error struct {
	Code   string
	Params []string
}

func (e *Error) Error() string {
	if len(e.Params) == 0 {
		return "xapi: " + e.Code
	}
	return fmt.Sprintf("xapi: %s %s", e.Code, strings.Join(e.Params, " "))
}

// IsCode reports whether err is an XAPI error with the given code.
func IsCode(err error, code string) bool {
	var xe *Error
	return errors.As(err, &xe) && xe.Code == code
}

type Client struct {
	cfg    *Config
	http   *http.Client
	tls    *tls.Config
	nextID atomic.Uint64

	mu      sync.Mutex
	base    string // https://host[:port]
	session string
}

// Dial logs in to the pool master. If the configured host is a pool member, it follows
// the HOST_IS_SLAVE answer to the master once.
func Dial(ctx context.Context, cfg *Config) (*Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: cfg.TLSInsecure} //nolint:gosec // opt-in, XCP-ng ships a self-signed certificate
	c := &Client{
		cfg:  cfg,
		base: "https://" + cfg.Host,
		http: &http.Client{Transport: transport},
		tls:  transport.TLSClientConfig,
	}
	if err := c.Relogin(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Relogin opens a new session, for when the old one is gone: xapi keeps no session
// across a toolstack restart, and the calls made with it fail with SESSION_INVALID.
// It goes back to the configured host, so it also finds a new master after a failover.
func (c *Client) Relogin(ctx context.Context) error {
	base := "https://" + c.cfg.Host
	session, err := c.login(ctx, base)
	var xe *Error
	if errors.As(err, &xe) && xe.Code == "HOST_IS_SLAVE" && len(xe.Params) == 1 {
		master := xe.Params[0]
		if _, port, splitErr := net.SplitHostPort(c.cfg.Host); splitErr == nil {
			master = net.JoinHostPort(master, port)
		}
		base = "https://" + master
		session, err = c.login(ctx, base)
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.base, c.session = base, session
	c.mu.Unlock()
	return nil
}

func (c *Client) login(ctx context.Context, base string) (string, error) {
	res, err := c.rpcAt(ctx, base, "session.login_with_password",
		[]any{c.cfg.Username, c.cfg.Password, "1.0", "plakar-xcpng"})
	if err != nil {
		return "", err
	}
	var session string
	err = json.Unmarshal(res, &session)
	return session, err
}

func (c *Client) current() (base, session string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base, c.session
}

// Close logs the session out.
func (c *Client) Close(ctx context.Context) error {
	_, session := c.current()
	if session == "" {
		return nil
	}
	_, err := c.rpc(ctx, "session.logout", []any{session})
	c.mu.Lock()
	c.session = ""
	c.mu.Unlock()
	return err
}

// unavailableError is a call that got no answer from xapi: the connection failed or
// broke, or a proxy in front of it had no upstream. The call may or may not have run.
type unavailableError struct{ err error }

func (e *unavailableError) Error() string { return e.err.Error() }
func (e *unavailableError) Unwrap() error { return e.err }

// IsUnavailable reports whether err is a call that xapi did not answer, as during a
// toolstack restart, rather than an error xapi returned.
func IsUnavailable(err error) bool {
	var ue *unavailableError
	return errors.As(err, &ue)
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      uint64 `json:"id"`
}

type rpcError struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Data    []string `json:"data"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

func (c *Client) rpc(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	base, _ := c.current()
	return c.rpcAt(ctx, base, method, params)
}

func (c *Client) rpcAt(ctx context.Context, base, method string, params []any) (json.RawMessage, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", Method: method, Params: params, ID: c.nextID.Add(1)})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/jsonrpc", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, &unavailableError{err})
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return nil, fmt.Errorf("%s: %w", method, &unavailableError{fmt.Errorf("HTTP %s", resp.Status)})
	default:
		return nil, fmt.Errorf("%s: HTTP %s", method, resp.Status)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, &unavailableError{err})
	}
	if len(data) > maxResponseSize {
		return nil, fmt.Errorf("%s: response larger than %d bytes", method, maxResponseSize)
	}

	var r rpcResponse
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("%s: decoding response: %w", method, err)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("%s: %w", method, &Error{Code: r.Error.Message, Params: r.Error.Data})
	}
	return r.Result, nil
}

// Call runs an API method with the session as first argument and decodes the result
// into out, which may be nil.
func (c *Client) Call(ctx context.Context, out any, method string, args ...any) error {
	_, session := c.current()
	res, err := c.rpc(ctx, method, append([]any{session}, args...))
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(res))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("%s: decoding result: %w", method, err)
	}
	return nil
}

func (c *Client) handlerURL(handler, vdiRef string) string {
	base, session := c.current()
	q := url.Values{}
	q.Set("session_id", session)
	q.Set("vdi", vdiRef)
	q.Set("format", "raw")
	return base + handler + "?" + q.Encode()
}

// ExportRawVDI streams the content of a VDI as a raw disk image of size bytes, its
// virtual size. xapi sends no Content-Length and ends the body by closing the
// connection, so a stream cut off early (toolstack restart, master failover, SR I/O
// error) reads like a clean end of disk: the reader returns an error unless exactly
// size bytes arrive.
func (c *Client) ExportRawVDI(ctx context.Context, vdiRef string, size int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.handlerURL("/export_raw_vdi", vdiRef), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("export_raw_vdi: %w", redactURL(err))
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("export_raw_vdi: HTTP %s", resp.Status)
	}
	return &exactReader{rc: resp.Body, want: size}, nil
}

// exactReader fails a stream that does not hold exactly want bytes.
type exactReader struct {
	rc   io.ReadCloser
	want int64
	got  int64
}

func (r *exactReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.got += int64(n)
	if r.got > r.want {
		return n, fmt.Errorf("export_raw_vdi: more than the %d bytes of the disk", r.want)
	}
	if errors.Is(err, io.EOF) && r.got < r.want {
		return n, fmt.Errorf("export_raw_vdi: stream ended after %d of %d bytes: %w", r.got, r.want, io.ErrUnexpectedEOF)
	}
	return n, err
}

func (r *exactReader) Close() error { return r.rc.Close() }

// ImportRawVDI writes size bytes from r into an existing VDI.
func (c *Client) ImportRawVDI(ctx context.Context, vdiRef string, size int64, r io.Reader) error {
	task, done, err := c.importTask(ctx)
	if err != nil {
		return err
	}
	defer done()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.handlerURL("/import_raw_vdi", vdiRef)+"&task_id="+url.QueryEscape(task), io.LimitReader(r, size))
	if err != nil {
		return err
	}
	req.ContentLength = size

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("import_raw_vdi: %w", redactURL(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("import_raw_vdi: HTTP %s", resp.Status)
	}
	// xapi sends no length and closes the connection once the write is over.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if err := c.importResult(ctx, task); err != nil {
		return fmt.Errorf("import_raw_vdi: %w", err)
	}
	return nil
}

// importTask creates the task an import runs under, and returns it with a function
// that destroys it. xapi answers an import with 200 before it writes the disk, and
// reports a failure of the write on that task. What it sends on the connection after
// the 200 varies: a 400, a 400 followed by a 500, or nothing before it closes, so the
// connection alone cannot tell a failed write from a complete one.
func (c *Client) importTask(ctx context.Context) (string, func(), error) {
	var task string
	if err := c.Call(ctx, &task, "task.create", "plakar import_raw_vdi", ""); err != nil {
		return "", nil, fmt.Errorf("import_raw_vdi: %w", err)
	}
	done := func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = c.Call(dctx, nil, "task.destroy", task)
	}
	return task, done, nil
}

// importTaskWait bounds how long importResult waits for a task that is still
// pending. xapi completes or fails the task before it closes the connection, so a
// pending task after the close is not expected.
var importTaskWait = 30 * time.Second

// importResult returns the outcome of an import task, once the connection is closed.
func (c *Client) importResult(ctx context.Context, task string) error {
	deadline := time.Now().Add(importTaskWait)
	for {
		var status string
		if err := c.Call(ctx, &status, "task.get_status", task); err != nil {
			return err
		}
		switch status {
		case "success":
			return nil
		case "failure":
			var info []string
			if err := c.Call(ctx, &info, "task.get_error_info", task); err != nil {
				return err
			}
			if len(info) == 0 {
				return errors.New("xapi failed the write with no error code")
			}
			return &Error{Code: info[0], Params: info[1:]}
		case "pending":
		default:
			return fmt.Errorf("xapi task ended %s", status)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("xapi task still pending %s after the connection closed", importTaskWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// sparseBlock is the unit in which ImportSparseVDI looks for zeroes: one VHD block.
const sparseBlock = 2 << 20

// ImportSparseVDI writes size bytes from r into an existing VDI and sends only the
// 2 MiB blocks that hold a non-zero byte, so a file-based SR allocates no block for
// the rest. The VDI must read as zeroes before the import (see SRZeroesNewVDIs). It
// returns the number of bytes sent.
//
// It uses xapi's chunked import (import_raw_vdi with chunked set): each chunk is a
// little-endian uint64 byte offset and uint32 length followed by the data, and an
// all-zero header ends the stream. xapi rejects a Transfer-Encoding and the body
// length is not known in advance, which net/http cannot send, so the request is
// written on the TLS connection directly.
func (c *Client) ImportSparseVDI(ctx context.Context, vdiRef string, size int64, r io.Reader) (int64, error) {
	return c.importChunked(ctx, vdiRef, func(w *bufio.Writer) (int64, error) {
		return writeChunks(w, size, r)
	})
}

// WriteVDIAt writes data into an existing VDI at byte offset off, in one chunk of
// xapi's chunked import, and leaves the rest of the VDI as it was. off and len(data)
// must be whole 512-byte sectors.
func (c *Client) WriteVDIAt(ctx context.Context, vdiRef string, off int64, data []byte) error {
	_, err := c.importChunked(ctx, vdiRef, func(w *bufio.Writer) (int64, error) {
		if err := writeChunk(w, off, data); err != nil {
			return 0, err
		}
		return int64(len(data)), writeChunk(w, 0, nil)
	})
	return err
}

// importChunked sends a chunked import_raw_vdi request whose chunks body writes, and
// returns what body reports as sent once xapi has finished the write.
func (c *Client) importChunked(ctx context.Context, vdiRef string, body func(*bufio.Writer) (int64, error)) (int64, error) {
	task, done, err := c.importTask(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	u, err := url.Parse(c.handlerURL("/import_raw_vdi", vdiRef) + "&chunked=true&task_id=" + url.QueryEscape(task))
	if err != nil {
		return 0, err
	}
	addr := u.Host
	if u.Port() == "" {
		addr = net.JoinHostPort(u.Hostname(), "443")
	}

	dialer := &tls.Dialer{Config: c.tls}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("import_raw_vdi: %w", err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	w := bufio.NewWriterSize(conn, 64<<10)
	var sent int64
	_, werr := fmt.Fprintf(w, "PUT %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", u.RequestURI(), u.Host)
	if werr == nil {
		werr = w.Flush()
	}
	if werr == nil {
		sent, werr = body(w)
	}
	if se := (*sourceError)(nil); errors.As(werr, &se) {
		// Closing the connection mid-stream makes xapi fail the import.
		return sent, se.err
	}
	if werr == nil {
		werr = w.Flush()
	}

	// xapi answers before it reads the body, so a refused request is reported
	// here even when writing the body failed.
	br := bufio.NewReader(conn)
	resp, rerr := http.ReadResponse(br, nil)
	if rerr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return sent, ctxErr
		}
		if werr != nil {
			return sent, fmt.Errorf("import_raw_vdi: %w", werr)
		}
		return sent, fmt.Errorf("import_raw_vdi: reading response: %w", rerr)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return sent, fmt.Errorf("import_raw_vdi: HTTP %s", resp.Status)
	}
	if werr != nil {
		return sent, fmt.Errorf("import_raw_vdi: %w", werr)
	}
	// xapi closes the connection once the write is over.
	_, _ = io.Copy(io.Discard, io.LimitReader(br, 64<<10))
	if err := c.importResult(ctx, task); err != nil {
		return sent, fmt.Errorf("import_raw_vdi: %w", err)
	}
	return sent, nil
}

// sourceError is a failure to read the disk being restored, as opposed to a failure
// to send it.
type sourceError struct{ err error }

func (e *sourceError) Error() string { return e.err.Error() }

// writeChunks writes size bytes from r as chunks, leaving out the 2 MiB blocks that
// are all zeroes, then the end marker.
func writeChunks(w *bufio.Writer, size int64, r io.Reader) (int64, error) {
	buf := make([]byte, sparseBlock)
	zero := make([]byte, sparseBlock)
	var sent int64
	for off := int64(0); off < size; {
		n := int(min(int64(sparseBlock), size-off))
		if _, err := io.ReadFull(r, buf[:n]); err != nil {
			return sent, &sourceError{fmt.Errorf("reading disk at offset %d: %w", off, err)}
		}
		if !bytes.Equal(buf[:n], zero[:n]) {
			if err := writeChunk(w, off, buf[:n]); err != nil {
				return sent, err
			}
			sent += int64(n)
		}
		off += int64(n)
	}
	return sent, writeChunk(w, 0, nil)
}

// writeChunk writes one chunk: its offset, its length and data. An empty chunk at
// offset 0 is the end marker.
func writeChunk(w *bufio.Writer, off int64, data []byte) error {
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[:8], uint64(off))
	binary.LittleEndian.PutUint32(hdr[8:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// SRZeroesNewVDIs reports whether a new VDI on an SR of this type reads as zeroes,
// which ImportSparseVDI relies on. It mirrors xapi's must_write_zeroes_into_new_vdi
// (ocaml/xapi/sm_fs_ops.ml): LVM SRs not upgraded to VHD, EqualLogic in thick mode
// and StorageLink do not zero new disks. The raw-VDI request that rule also checks
// is a statefile setting this connector never sets.
func SRZeroesNewVDIs(srType string, smConfig map[string]string) bool {
	switch strings.ToLower(srType) {
	case "lvm", "lvmoiscsi", "lvmohba":
		return smConfig["use_vhd"] == "true"
	case "equal":
		for k, v := range smConfig {
			if strings.EqualFold(k, "allocation") && strings.EqualFold(v, "thick") {
				return false
			}
		}
		return true
	case "cslg":
		return false
	}
	return true
}

// redactURL drops the request URL from an HTTP client error: the export and import
// URLs carry the session id in their query string.
func redactURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
