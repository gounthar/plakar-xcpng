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
	// It also bounds the destroy that follows, which opens a new TLS connection after the
	// dropped one: under riscv64 emulation that takes longer than 50 ms.
	cleanupRetry = time.Second
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

// A VDI xapi holds for longer than the watchdog has must not keep the next one from
// being destroyed.
func TestOneHeldVDIDoesNotBlockTheOthers(t *testing.T) {
	old := restorePollDelay
	restorePollDelay = time.Millisecond
	t.Cleanup(func() { restorePollDelay = old })
	f := &restoreXAPI{held: map[string]bool{"OpaqueRef:held": true}}
	p := restoreExporter(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := p.destroyListed(ctx, []string{"vdi/OpaqueRef:held", "vdi/OpaqueRef:b"})
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:b"}) {
		t.Fatalf("destroyed %q, want the VDI after the held one", f.destroyed)
	}
	if err == nil || !strings.Contains(err.Error(), "vdi/OpaqueRef:held") || !strings.Contains(err.Error(), "VDI_IN_USE") {
		t.Fatalf("error %v does not name the held VDI and why", err)
	}
}

// A VDI held ahead of a free one: the free one goes first, the held one once xapi
// lets it go.
func TestAHeldVDIGoesOnceReleased(t *testing.T) {
	old := restorePollDelay
	restorePollDelay = time.Millisecond
	t.Cleanup(func() { restorePollDelay = old })
	f := &restoreXAPI{heldFor: map[string]int{"OpaqueRef:held": 2}}
	p := restoreExporter(t, f)
	if err := p.destroyListed(context.Background(), []string{"vdi/OpaqueRef:held", "vdi/OpaqueRef:b"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:b", "OpaqueRef:held"}) || f.heldFor["OpaqueRef:held"] != 0 {
		t.Fatalf("destroyed %q with %d busy answers left", f.destroyed, f.heldFor["OpaqueRef:held"])
	}
}

// A destroy started just before the watchdog's time is up, against a host that stopped
// answering, must not retry past that time.
func TestWatchdogRetryEndsWithItsTime(t *testing.T) {
	f := &restoreXAPI{dropDestroy: true}
	p := restoreExporter(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.destroyListed(ctx, []string{"vdi/OpaqueRef:a"})
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("destroyListed took %s past a 200ms deadline", took)
	}
	if err == nil || !strings.Contains(err.Error(), "vdi/OpaqueRef:a") {
		t.Fatalf("error %v does not name the VDI left", err)
	}
}

// A held VDI whose last try is cut by the watchdog's time is still reported as held.
func TestCutTryKeepsTheBusyReason(t *testing.T) {
	f := &restoreXAPI{heldFor: map[string]int{"OpaqueRef:a": 1}}
	p := restoreExporter(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// The first try is answered VDI_IN_USE; every try after it goes unanswered.
	f.afterHeld = true
	err := p.destroyListed(ctx, []string{"vdi/OpaqueRef:a"})
	if err == nil || !strings.Contains(err.Error(), "VDI_IN_USE") {
		t.Fatalf("error %v does not say the VDI was held", err)
	}
}

// A restore failing right after a cut import gets VDI_IN_USE from its own cleanup:
// it waits for xapi to let the VDI go rather than leave it to the watchdog, which
// would report it as left by an interrupted restore.
func TestInProcessCleanupWaitsForAHeldVDI(t *testing.T) {
	f := &restoreXAPI{}
	p, rec := recordedExporter(t, f)
	restored := restoreOneDisk(t, p)
	f.heldFor = map[string]int{"OpaqueRef:vdi0": 2}
	if err := p.destroyDisks(context.Background(), sbVM, restored); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.destroyed, []string{"OpaqueRef:vdi0"}) {
		t.Fatalf("destroyed %q, want OpaqueRef:vdi0", f.destroyed)
	}
	if got := rec.pending(t); len(got) != 0 {
		t.Fatalf("left %q listed for the watchdog", got)
	}
}

// An interrupted restore does not wait for a held VDI: the user asked it to stop, and
// the watchdog destroys the VDI once the plugin exits.
func TestInterruptedCleanupLeavesAHeldVDIToTheWatchdog(t *testing.T) {
	f := &restoreXAPI{}
	p, rec := recordedExporter(t, f)
	restored := restoreOneDisk(t, p)
	f.heldFor = map[string]int{"OpaqueRef:vdi0": 1 << 20}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := p.destroyDisks(ctx, sbVM, restored)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("an interrupted cleanup waited %s for a held VDI", took)
	}
	if err == nil || !strings.Contains(err.Error(), "VDI_IN_USE") {
		t.Fatalf("error %v does not say the VDI was held", err)
	}
	if got := rec.pending(t); !slices.Equal(got, []string{"vdi/OpaqueRef:vdi0"}) {
		t.Fatalf("listed %q, want the held VDI left to the watchdog", got)
	}
}
