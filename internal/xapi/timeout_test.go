package xapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A host that accepts the connection and never answers must not hold a call forever
// when the caller's context has no deadline.
func TestCallWithoutDeadlineIsBounded(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server sees the client hang up only once the body is read.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	old := callTimeout
	callTimeout = 200 * time.Millisecond
	t.Cleanup(func() { callTimeout = old })

	c := &Client{base: srv.URL, http: srv.Client(), session: "OpaqueRef:session"}
	start := time.Now()
	err := c.Call(context.Background(), nil, "VM.get_all")
	if err == nil {
		t.Fatal("call succeeded with the host not answering")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("gave up after %s, want about %s", d, callTimeout)
	}
	if !IsUnavailable(err) {
		t.Errorf("error %v is not reported as unavailable", err)
	}
}

// A deadline the caller set is kept, even when it is longer than callTimeout.
func TestCallKeepsCallerDeadline(t *testing.T) {
	answered := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
		close(answered)
	}))
	t.Cleanup(srv.Close)
	old := callTimeout
	callTimeout = 100 * time.Millisecond
	t.Cleanup(func() { callTimeout = old })

	c := &Client{base: srv.URL, http: srv.Client(), session: "OpaqueRef:session"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out string
	if err := c.Call(ctx, &out, "VM.get_all"); err != nil || out != "ok" {
		t.Fatalf("Call = %q, %v; want ok with the caller's longer deadline", out, err)
	}
	<-answered
}
