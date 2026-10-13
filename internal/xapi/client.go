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
	"sync/atomic"
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
	base    string // https://host[:port]
	http    *http.Client
	tls     *tls.Config
	session string
	nextID  atomic.Uint64
}

// Dial logs in to the pool master. If the configured host is a pool member, it follows
// the HOST_IS_SLAVE answer to the master once.
func Dial(ctx context.Context, cfg *Config) (*Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: cfg.TLSInsecure} //nolint:gosec // opt-in, XCP-ng ships a self-signed certificate
	c := &Client{
		base: "https://" + cfg.Host,
		http: &http.Client{Transport: transport},
		tls:  transport.TLSClientConfig,
	}

	err := c.login(ctx, cfg)
	var xe *Error
	if errors.As(err, &xe) && xe.Code == "HOST_IS_SLAVE" && len(xe.Params) == 1 {
		master := xe.Params[0]
		if _, port, splitErr := net.SplitHostPort(cfg.Host); splitErr == nil {
			master = net.JoinHostPort(master, port)
		}
		c.base = "https://" + master
		err = c.login(ctx, cfg)
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) login(ctx context.Context, cfg *Config) error {
	res, err := c.rpc(ctx, "session.login_with_password", []any{cfg.Username, cfg.Password, "1.0", "plakar-xcpng"})
	if err != nil {
		return err
	}
	return json.Unmarshal(res, &c.session)
}

// Close logs the session out.
func (c *Client) Close(ctx context.Context) error {
	if c.session == "" {
		return nil
	}
	_, err := c.rpc(ctx, "session.logout", []any{c.session})
	c.session = ""
	return err
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
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", Method: method, Params: params, ID: c.nextID.Add(1)})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/jsonrpc", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %s", method, resp.Status)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
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
	res, err := c.rpc(ctx, method, append([]any{c.session}, args...))
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
	q := url.Values{}
	q.Set("session_id", c.session)
	q.Set("vdi", vdiRef)
	q.Set("format", "raw")
	return c.base + handler + "?" + q.Encode()
}

// ExportRawVDI streams the content of a VDI as a raw disk image.
func (c *Client) ExportRawVDI(ctx context.Context, vdiRef string) (io.ReadCloser, error) {
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
	return resp.Body, nil
}

// ImportRawVDI writes size bytes from r into an existing VDI.
func (c *Client) ImportRawVDI(ctx context.Context, vdiRef string, size int64, r io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.handlerURL("/import_raw_vdi", vdiRef), io.LimitReader(r, size))
	if err != nil {
		return err
	}
	req.ContentLength = size

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("import_raw_vdi: %w", redactURL(err))
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("import_raw_vdi: HTTP %s", resp.Status)
	}
	return nil
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
	u, err := url.Parse(c.handlerURL("/import_raw_vdi", vdiRef) + "&chunked=true")
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
	sent, werr := writeChunked(w, u, size, r)
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
	// xapi closes the connection once the data is written to the VDI.
	if _, err := io.Copy(io.Discard, io.LimitReader(br, 64<<10)); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return sent, ctxErr
		}
		return sent, fmt.Errorf("import_raw_vdi: %w", err)
	}
	return sent, nil
}

// sourceError is a failure to read the disk being restored, as opposed to a failure
// to send it.
type sourceError struct{ err error }

func (e *sourceError) Error() string { return e.err.Error() }

func writeChunked(w *bufio.Writer, u *url.URL, size int64, r io.Reader) (int64, error) {
	if _, err := fmt.Fprintf(w, "PUT %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", u.RequestURI(), u.Host); err != nil {
		return 0, err
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}

	buf := make([]byte, sparseBlock)
	zero := make([]byte, sparseBlock)
	var hdr [12]byte
	var sent int64
	for off := int64(0); off < size; {
		n := int(min(int64(sparseBlock), size-off))
		if _, err := io.ReadFull(r, buf[:n]); err != nil {
			return sent, &sourceError{fmt.Errorf("reading disk at offset %d: %w", off, err)}
		}
		if !bytes.Equal(buf[:n], zero[:n]) {
			binary.LittleEndian.PutUint64(hdr[:8], uint64(off))
			binary.LittleEndian.PutUint32(hdr[8:], uint32(n))
			if _, err := w.Write(hdr[:]); err != nil {
				return sent, err
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return sent, err
			}
			sent += int64(n)
		}
		off += int64(n)
	}
	clear(hdr[:])
	_, err := w.Write(hdr[:])
	return sent, err
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
