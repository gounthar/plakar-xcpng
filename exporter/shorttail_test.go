package exporter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// xapiPage is the error page found at the end of a disk stored by an export that
// xapi cut short, on XCP-ng 8.3 (xapi 26.1.16), byte for byte.
const xapiPage = "HTTP/1.1 500 Internal Error\r\ncontent-length: 266\r\ncontent-type: text/html\r\nconnection: close\r\ncache-control: no-cache, no-store\r\n\r\n<html><body><h1>HTTP 500 internal server error</h1>An unexpected error occurred; please wait a while and try again. If the problem persists, please contact your support representative.<h1> Additional information </h1>VDI_IO_ERROR: [ Device I/O errors ]</body></html>"

func randomBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range b {
		b[i] = byte(r.UintN(255) + 1) // no zero byte, so a zeroed range shows
	}
	return b
}

// readTail drains a shortTail over file, read through each of the given wrappers.
func readTail(t *testing.T, file []byte, wrap func(io.Reader) io.Reader) (*shortTail, []byte) {
	t.Helper()
	tail := newShortTail(wrap(bytes.NewReader(file)), int64(len(file)))
	out, err := io.ReadAll(tail)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return tail, out
}

var wrappers = map[string]func(io.Reader) io.Reader{
	"plain":    func(r io.Reader) io.Reader { return r },
	"half":     iotest.HalfReader,
	"datalast": iotest.DataErrReader,
}

func TestShortTailStripsErrorPage(t *testing.T) {
	for _, n := range []int{600*sector + 194, 10*sector + 194} { // longer and shorter than tailHold
		data := randomBytes(n)
		file := append(bytes.Clone(data), xapiPage...)
		for name, wrap := range wrappers {
			tail, out := readTail(t, file, wrap)
			wantLen := len(file) - len(file)%sector
			kept := n - n%sector
			if len(out) != wantLen {
				t.Errorf("%d/%s: yielded %d bytes, want %d", n, name, len(out), wantLen)
				continue
			}
			if !bytes.Equal(out[:kept], data[:kept]) {
				t.Errorf("%d/%s: disk data changed", n, name)
			}
			if !bytes.Equal(out[kept:], make([]byte, wantLen-kept)) {
				t.Errorf("%d/%s: bytes from %d on are not zeroes", n, name, kept)
			}
			if tail.kept != int64(kept) || tail.page != len(xapiPage) {
				t.Errorf("%d/%s: kept %d page %d, want %d %d", n, name, tail.kept, tail.page, kept, len(xapiPage))
			}
			if d := tail.dropped(); !strings.Contains(d, "397-byte xapi error page") || !strings.Contains(d, " 194 bytes ") {
				t.Errorf("%d/%s: dropped = %q", n, name, d)
			}
		}
	}
}

func TestShortTailRoundsDownWithoutPage(t *testing.T) {
	file := randomBytes(600*sector + 79)
	for name, wrap := range wrappers {
		tail, out := readTail(t, file, wrap)
		limit := len(file) - 79
		if !bytes.Equal(out, file[:limit]) {
			t.Errorf("%s: yielded %d bytes, want the first %d unchanged", name, len(out), limit)
		}
		if tail.page != 0 || tail.kept != int64(limit) {
			t.Errorf("%s: kept %d page %d", name, tail.kept, tail.page)
		}
		if d := tail.dropped(); !strings.Contains(d, "last 79 bytes") {
			t.Errorf("%s: dropped = %q", name, d)
		}
	}
}

func TestShortTailLeavesWholeDiskAlone(t *testing.T) {
	file := randomBytes(64 * sector * 10)
	tail, out := readTail(t, file, wrappers["half"])
	if !bytes.Equal(out, file) || tail.dropped() != "" {
		t.Errorf("whole disk: changed=%v dropped=%q", !bytes.Equal(out, file), tail.dropped())
	}
}

// Only a response that ends exactly at the end of the file is xapi's page: one with
// bytes after it, a body shorter than its Content-Length, or a 200 is disk data.
func TestShortTailKeepsPageLookalikes(t *testing.T) {
	ok := strings.Replace(xapiPage, "500 Internal Error", "200 OK", 1)
	for name, end := range map[string]string{
		"bytes after": xapiPage + "x",
		"cut body":    xapiPage[:len(xapiPage)-1],
		"status 200":  ok,
	} {
		file := append(randomBytes(300_000), end...)
		tail, out := readTail(t, file, wrappers["plain"])
		limit := len(file) - len(file)%sector
		if tail.page != 0 || !bytes.Equal(out, file[:limit]) {
			t.Errorf("%s: page %d, data changed=%v", name, tail.page, !bytes.Equal(out, file[:limit]))
		}
	}
}

func TestShortTailReportsShortSource(t *testing.T) {
	file := randomBytes(300_000)
	tail := newShortTail(bytes.NewReader(file[:200_000]), int64(len(file)))
	if _, err := io.ReadAll(tail); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("source shorter than its size: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// The end of a disk is guest data. A tail packed with response headers must not make
// the search slow, and a response whose body spans more than maxPage is not a page.
func TestShortTailBoundsTheSearch(t *testing.T) {
	fake := "HTTP/1.1 500 x\r\nContent-Length: 999999\r\n\r\n"
	packed := append(randomBytes(10*sector), strings.Repeat(fake, (1<<20)/len(fake))...)
	long := strings.Repeat("y", 2*maxPage)
	spanning := append(randomBytes(600*sector), fmt.Sprintf("HTTP/1.1 500 x\r\nContent-Length: %d\r\n\r\n%s", len(long), long)...)
	for name, file := range map[string][]byte{"packed": packed, "spanning": spanning} {
		start := time.Now()
		tail, out := readTail(t, file, wrappers["plain"])
		limit := len(file) - len(file)%sector
		if tail.page != 0 || !bytes.Equal(out, file[:limit]) {
			t.Errorf("%s: page %d, data changed=%v", name, tail.page, !bytes.Equal(out, file[:limit]))
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("%s: took %v", name, d)
		}
	}
}
