package importer

import (
	"errors"
	"sync"

	"github.com/PlakarKorp/kloset/connectors"
)

var errResultsClosed = errors.New("results channel closed before the record was acknowledged")

// ackTracker matches the results Plakar sends back (FLAG_NEEDACK) to the records the
// importer sent, so that a VM's snapshot is destroyed only once every one of its
// records has been read.
type ackTracker struct {
	mu      sync.Mutex
	waiting map[string]chan error
	closed  bool
}

func newAckTracker() *ackTracker {
	return &ackTracker{waiting: make(map[string]chan error)}
}

// expect registers a pathname before its record is sent. The returned channel gets
// exactly one value: the record's result error, or errResultsClosed.
func (t *ackTracker) expect(pathname string) <-chan error {
	ch := make(chan error, 1)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		ch <- errResultsClosed
		return ch
	}
	t.waiting[pathname] = ch
	return ch
}

// run consumes results until the channel is closed. It must run for the whole of
// Import(), or Plakar's workers block on a full results channel.
func (t *ackTracker) run(results <-chan *connectors.Result) {
	for res := range results {
		t.mu.Lock()
		ch, ok := t.waiting[res.Record.Pathname]
		delete(t.waiting, res.Record.Pathname)
		t.mu.Unlock()
		if ok {
			ch <- res.Err
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for p, ch := range t.waiting {
		ch <- errResultsClosed
		delete(t.waiting, p)
	}
}

// forget drops a registration for a record that was never sent.
func (t *ackTracker) forget(pathname string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.waiting, pathname)
}
