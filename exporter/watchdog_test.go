package exporter

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"integration-xcpng/internal/watchdog"
)

// recordWatchdog stands in for the watchdog child and records the protocol lines.
type recordWatchdog struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordWatchdog) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")...)
	return len(b), nil
}

func (r *recordWatchdog) Close() error { return nil }

// pending is what the watchdog would destroy if the plugin died now.
func (r *recordWatchdog) pending(t *testing.T) []string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	_, pending, err := watchdog.Read(strings.NewReader("{}\n" + strings.Join(r.lines, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	return pending
}

func recordedExporter(t *testing.T, f *restoreXAPI) (*Exporter, *recordWatchdog) {
	t.Helper()
	p := restoreExporter(t, f)
	rec := &recordWatchdog{}
	p.watchdog = watchdog.FromWriter(rec)
	return p, rec
}

// restoreOneDisk restores disk "a" of sbVM, the way Export does before the VMs.
func restoreOneDisk(t *testing.T, p *Exporter) map[string]restoredDisk {
	t.Helper()
	disk, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096)))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]restoredDisk{"/vm/a": disk}
}

// A plugin killed between a disk's restore and its VM's leaves the disk to the watchdog.
func TestRestoredDiskIsListedUntilItsVM(t *testing.T) {
	p, rec := recordedExporter(t, &restoreXAPI{})
	restoreOneDisk(t, p)
	if got := rec.pending(t); !slices.Equal(got, []string{"vdi/OpaqueRef:vdi0"}) {
		t.Fatalf("listed %q, want the restored VDI", got)
	}
}

func TestRestoredVMIsUnlisted(t *testing.T) {
	p, rec := recordedExporter(t, &restoreXAPI{})
	restored := restoreOneDisk(t, p)
	if err := p.createVM(context.Background(), sbVM, restored, true); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(rec.lines, "+vm/OpaqueRef:vm1") {
		t.Errorf("the VM was never listed: %q", rec.lines)
	}
	if got := rec.pending(t); len(got) != 0 {
		t.Fatalf("a restored VM leaves %q listed: a later kill would destroy it", got)
	}
}

func TestHeldVMIsUnlisted(t *testing.T) {
	f := &restoreXAPI{sbReadiness: "first_boot"}
	p, rec := recordedExporter(t, f)
	restored := restoreOneDisk(t, p)
	if err := p.createVM(context.Background(), sbVM, restored, true); err == nil {
		t.Fatal("createVM succeeded, want the Secure Boot hold")
	}
	if got := rec.pending(t); len(got) != 0 {
		t.Fatalf("a held VM leaves %q listed", got)
	}
}

func TestDestroyedVMIsUnlisted(t *testing.T) {
	old := cleanupRetry
	cleanupRetry = 50 * time.Millisecond
	t.Cleanup(func() { cleanupRetry = old })
	p, rec := recordedExporter(t, &restoreXAPI{dropSB: true})
	restored := restoreOneDisk(t, p)
	if err := p.createVM(context.Background(), sbVM, restored, true); err == nil {
		t.Fatal("createVM succeeded although the Secure Boot check got no answer")
	}
	if got := rec.pending(t); len(got) != 0 {
		t.Fatalf("destroyed objects left listed: %q", got)
	}
}

func TestWatchdogDestroysVMsFirst(t *testing.T) {
	f := &restoreXAPI{
		vdiLabels: map[string]string{"OpaqueRef:vdiL": "L"},
		vmMarkers: map[string]string{"OpaqueRef:vmM": "M"},
	}
	p := restoreExporter(t, f)
	keys := []string{"vdi/OpaqueRef:a", "vdi-label/OpaqueRef:sr/L", "vm/OpaqueRef:vm9", `vm-label/M/"web (restored)"`}
	if err := p.destroyListed(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	want := []string{"OpaqueRef:vm9", "OpaqueRef:vmM", "OpaqueRef:a", "OpaqueRef:vdiL"}
	if !slices.Equal(f.destroyed, want) {
		t.Fatalf("destroyed %q, want %q", f.destroyed, want)
	}
}

func TestIsWatchdog(t *testing.T) {
	if !IsWatchdog([]string{"xcpngExporter", WatchdogArg}) {
		t.Error("watchdog argument not recognised")
	}
	if IsWatchdog([]string{"xcpngExporter"}) {
		t.Error("plugin invocation taken for the watchdog")
	}
}

// A VM name comes from the backup. One holding a line break must not add a line of its
// own to the list, here one naming a VDI the restore never made.
func TestVMNameCannotWriteProtocolLines(t *testing.T) {
	p, rec := recordedExporter(t, &restoreXAPI{})
	evil := pendingVM{dir: "/vm", meta: sbVM.meta}
	evil.meta.VM = map[string]any{"name_label": "x\n+vdi/OpaqueRef:victim"}
	_ = p.createVM(context.Background(), evil, map[string]restoredDisk{"/vm/a": sbDisks["/vm/a"]}, true)
	for _, l := range rec.lines {
		if strings.Contains(l, "victim") && !strings.HasPrefix(l, "+vm-label/") && !strings.HasPrefix(l, "-vm-label/") {
			t.Fatalf("the VM name wrote a protocol line of its own: %q", rec.lines)
		}
	}
	if got := rec.pending(t); slices.Contains(got, "vdi/OpaqueRef:victim") {
		t.Fatalf("listed %q", got)
	}
}

// The watchdog starts as the plugin dies, while xapi may still hold the VDI it was
// importing into.
func TestWatchdogWaitsForABusyVDI(t *testing.T) {
	f := &restoreXAPI{inUse: 3}
	p := restoreExporter(t, f)
	if err := p.destroyListed(context.Background(), []string{"vdi/OpaqueRef:a"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:a"}) || f.inUse != 0 {
		t.Fatalf("destroyed %q with %d busy answers left", f.destroyed, f.inUse)
	}
}

// Past the watchdog's time, a key left is reported, not given another round of retries.
func TestWatchdogStopsWhenItsTimeIsUp(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	keys := []string{"vdi/OpaqueRef:a", "vm/OpaqueRef:vm9", "vdi-label/OpaqueRef:sr/L"}
	err := p.destroyListed(ctx, keys)
	if len(f.destroyed) != 0 {
		t.Fatalf("destroyed %q after the deadline", f.destroyed)
	}
	for _, key := range keys {
		if err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("error %v does not name %s", err, key)
		}
	}
}
