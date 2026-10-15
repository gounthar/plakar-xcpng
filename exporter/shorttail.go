package exporter

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// A backup made before #27 was fixed can hold a disk whose export xapi cut short. The
// stored file then ends wherever the export stopped, often mid-sector, and can end
// with the HTTP error page xapi wrote on the same connection after the 200. Neither
// is disk data, and vhd-tool refuses a chunk that is not a whole number of sectors.

const sector = 512

// maxPage is the longest error page shortTail looks for. xapi's is about 400 bytes
// on XCP-ng 8.3. The end of a disk is guest data, so the search stays this short:
// it costs at most a few parses of maxPage bytes, whatever the guest wrote there.
const maxPage = 4 << 10

// tailHold is how much of the end of a disk shortTail keeps back until the source
// ends. It must exceed maxPage plus a sector, so that the sector holding the start
// of a page has not been yielded yet.
const tailHold = 64 << 10

// tailRead is the most shortTail reads from its source at once.
const tailRead = 1 << 20

// shortTail reads a stored disk of size bytes and yields its first limit bytes: size
// rounded down to a whole sector. It holds back the last tailHold bytes until the
// source ends, then looks for an xapi HTTP error page ending exactly at the end of
// the file. From the start of the sector holding the page's first byte on, it yields
// zeroes, which on an SR that zeroes new VDIs reads the same as not writing them.
//
// Once the reader is drained, kept is the number of bytes of disk data it yielded and
// page the length of the error page it found, 0 if none. cut holds the stored bytes
// from kept to the end of the file that it zeroed: a complete disk can end with guest
// data shaped like the page, and only vm.json, read later, tells the two apart.
type shortTail struct {
	r     io.Reader
	size  int64
	limit int64
	out   int64  // bytes yielded so far
	buf   []byte // bytes read and not yet yielded
	eof   bool
	kept  int64
	page  int
	cut   []byte
}

func newShortTail(r io.Reader, size int64) *shortTail {
	limit := size - size%sector
	return &shortTail{r: r, size: size, limit: limit, kept: limit}
}

func (t *shortTail) Read(p []byte) (int, error) {
	for {
		if t.out >= t.limit {
			return 0, io.EOF
		}
		ready := len(t.buf)
		if !t.eof {
			ready -= tailHold
		}
		if ready > 0 {
			n := min(ready, len(p), int(min(t.limit-t.out, int64(len(t.buf)))))
			copy(p, t.buf[:n])
			t.buf = t.buf[n:]
			t.out += int64(n)
			return n, nil
		}
		if t.eof {
			return 0, io.ErrUnexpectedEOF
		}
		if err := t.fill(); err != nil {
			return 0, err
		}
	}
}

// fill reads up to tailRead more bytes from the source, and inspects the tail once
// the source ends.
func (t *shortTail) fill() error {
	if cap(t.buf)-len(t.buf) < tailRead {
		nb := make([]byte, len(t.buf), tailHold+2*tailRead)
		copy(nb, t.buf)
		t.buf = nb
	}
	n, err := t.r.Read(t.buf[len(t.buf) : len(t.buf)+tailRead])
	t.buf = t.buf[:len(t.buf)+n]
	switch err {
	case nil:
		return nil
	case io.EOF:
		t.eof = true
		t.trim()
		return nil
	default:
		return err
	}
}

// trim runs once the source has ended, with t.buf holding the end of the file. The
// page cannot start in bytes already yielded: they were followed by at least
// tailHold more.
func (t *shortTail) trim() {
	w := max(0, len(t.buf)-maxPage)
	i := errorPageAt(t.buf[w:])
	if i < 0 {
		return
	}
	i += w
	start := t.out + int64(i)
	if start-start%sector < t.out {
		return // cannot happen while tailHold exceeds maxPage plus a sector; kept for safety
	}
	t.page = len(t.buf) - i
	t.kept = min(t.limit, start-start%sector)
	if from := t.kept - t.out; from < int64(len(t.buf)) {
		t.cut = bytes.Clone(t.buf[from:])
		clear(t.buf[from:])
	}
}

// errorPageAt returns where in tail an HTTP response with a non-200 status starts
// whose body, by its Content-Length, ends exactly at the end of tail; -1 if none.
func errorPageAt(tail []byte) int {
	for i := bytes.LastIndex(tail, []byte("HTTP/1.")); i >= 0; i = bytes.LastIndex(tail[:i], []byte("HTTP/1.")) {
		src := bytes.NewReader(tail[i:])
		br := bufio.NewReader(src)
		resp, err := http.ReadResponse(br, nil)
		if err != nil || resp.StatusCode == http.StatusOK || resp.ContentLength < 0 {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		if err == nil && int64(len(body)) == resp.ContentLength && src.Len()+br.Buffered() == 0 {
			return i
		}
	}
	return -1
}

// dropped describes what the reader left out of the stored file, "" if nothing.
func (t *shortTail) dropped() string {
	if t.kept == t.size {
		return ""
	}
	if t.page == 0 {
		return fmt.Sprintf("its last %d bytes, short of a whole %d-byte sector, were not restored", t.size-t.kept, sector)
	}
	return fmt.Sprintf("the stored file ended with a %d-byte xapi error page; it and the %d bytes before it in the same sector were not restored",
		t.page, t.size-int64(t.page)-t.kept)
}
