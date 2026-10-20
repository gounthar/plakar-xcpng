//go:build labhook

package exporter

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLabPause(t *testing.T) {
	labPauseDir = t.TempDir()
	pause := filepath.Join(labPauseDir, "pause-vm-created")

	// No pause file: no stop, no marker.
	labPause("vm-created")
	if _, err := os.Stat(filepath.Join(labPauseDir, "at-vm-created")); err == nil {
		t.Fatal("at-vm-created written without a pause file")
	}

	if err := os.WriteFile(pause, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { labPause("vm-created"); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(labPauseDir, "at-vm-created")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("at-vm-created never written")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("labPause returned while the pause file exists")
	case <-time.After(300 * time.Millisecond):
	}

	if err := os.Remove(pause); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("labPause did not return after the pause file was removed")
	}
}
