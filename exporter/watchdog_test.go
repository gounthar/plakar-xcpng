package exporter

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"integration-xcpng/internal/watchdog"
	"integration-xcpng/internal/xapi"
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

// The watchdog destroys the task of an import the plugin was killed in, by its name. A
// name with no task, the plugin killed before task.create, is nothing to report.
func TestWatchdogDestroysLabelledTask(t *testing.T) {
	f := &restoreXAPI{tasks: map[string]string{"OpaqueRef:t1": "plakar import_raw_vdi T", "OpaqueRef:t2": "T"}}
	p := restoreExporter(t, f)
	out := stderrOf(t, func() {
		if err := p.destroyListed(context.Background(), []string{taskLabelKey("T"), taskLabelKey("none")}); err != nil {
			t.Fatal(err)
		}
	})
	if !slices.Equal(f.tasksDestroyed, []string{"OpaqueRef:t1"}) {
		t.Fatalf("destroyed tasks %q, want OpaqueRef:t1", f.tasksDestroyed)
	}
	if !strings.Contains(out, "cleaned up task-label/T ") || strings.Contains(out, "task-label/none") {
		t.Errorf("said:\n%s", out)
	}
}

// A task the plugin created and was killed before using stays pending: xapi only marks
// it on task.destroy. The watchdog gives it taskEndWait to end, then sets it cancelled
// and destroys it, and says it cleaned it up only then.
func TestWatchdogCancelsAnUnusedTask(t *testing.T) {
	old, oldPoll := taskEndWait, restorePollDelay
	taskEndWait, restorePollDelay = 50*time.Millisecond, time.Millisecond
	t.Cleanup(func() { taskEndWait, restorePollDelay = old, oldPoll })
	f := &restoreXAPI{
		tasks:      map[string]string{"OpaqueRef:t1": "plakar import_raw_vdi T"},
		taskStatus: map[string]string{"OpaqueRef:t1": "pending"},
	}
	p := restoreExporter(t, f)
	start := time.Now()
	if err := p.destroyListed(context.Background(), []string{taskLabelKey("T")}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < taskEndWait {
		t.Errorf("cancelled after %s, before taskEndWait (%s)", took, taskEndWait)
	}
	if f.taskStatus["OpaqueRef:t1"] != "cancelled" || !slices.Contains(f.tasksDestroyed, "OpaqueRef:t1") {
		t.Fatalf("status %q, destroyed %q", f.taskStatus["OpaqueRef:t1"], f.tasksDestroyed)
	}
}

// A task that ends within taskEndWait, as a cut import's does, is destroyed, never
// cancelled.
func TestWatchdogWaitsForATaskToEnd(t *testing.T) {
	old, oldPoll := taskEndWait, restorePollDelay
	taskEndWait, restorePollDelay = time.Minute, time.Millisecond
	t.Cleanup(func() { taskEndWait, restorePollDelay = old, oldPoll })
	f := &restoreXAPI{
		tasks:      map[string]string{"OpaqueRef:t1": "plakar import_raw_vdi T"},
		taskStatus: map[string]string{"OpaqueRef:t1": "pending"},
	}
	p := restoreExporter(t, f)
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.mu.Lock()
		f.taskStatus["OpaqueRef:t1"] = "failure"
		f.mu.Unlock()
	}()
	if err := p.destroyListed(context.Background(), []string{taskLabelKey("T")}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.taskStatus["OpaqueRef:t1"] != "failure" || !slices.Contains(f.tasksDestroyed, "OpaqueRef:t1") {
		t.Fatalf("status %q, destroyed %q", f.taskStatus["OpaqueRef:t1"], f.tasksDestroyed)
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
// would report it only once the restore has ended.
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

// A VM's held disks share one wait: three held for good cost one heldWait, not three.
func TestHeldDisksShareOneWait(t *testing.T) {
	old, oldRestore := heldWait, restoreWait
	heldWait, restoreWait = 300*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { heldWait, restoreWait = old, oldRestore })
	f := &restoreXAPI{held: map[string]bool{"OpaqueRef:a": true, "OpaqueRef:b": true, "OpaqueRef:c": true}}
	p := restoreExporter(t, f)
	vm := pendingVM{dir: "/vm", meta: xapi.VMMeta{Disks: []xapi.DiskMeta{{VDIUUID: "a"}, {VDIUUID: "b"}, {VDIUUID: "c"}}}}
	restored := map[string]restoredDisk{"/vm/a": {ref: "OpaqueRef:a"}, "/vm/b": {ref: "OpaqueRef:b"}, "/vm/c": {ref: "OpaqueRef:c"}}
	start := time.Now()
	err := p.destroyDisks(context.Background(), vm, restored)
	if took := time.Since(start); took > 750*time.Millisecond {
		t.Fatalf("three held disks took %s, want about one heldWait (%s)", took, heldWait)
	}
	for _, ref := range []string{"OpaqueRef:a", "OpaqueRef:b", "OpaqueRef:c"} {
		if err == nil || !strings.Contains(err.Error(), ref+": ") {
			t.Fatalf("error %v does not name %s", err, ref)
		}
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

// deadWatchdog stands in for a watchdog child that has exited: every write fails.
type deadWatchdog struct{}

func (deadWatchdog) Write([]byte) (int, error) { return 0, os.ErrClosed }
func (deadWatchdog) Close() error              { return os.ErrClosed }

// stderrOf returns what f writes to standard error.
func stderrOf(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// Read as it is written: a warning longer than the pipe holds would block f.
	read := make(chan []byte)
	go func() {
		out, _ := io.ReadAll(r)
		read <- out
	}()
	saved := os.Stderr
	os.Stderr = w
	// A t.Fatal in f ends this goroutine without returning: put stderr back and
	// end the reader all the same.
	defer func() {
		os.Stderr = saved
		_ = w.Close()
	}()
	f()
	os.Stderr = saved
	_ = w.Close()
	return string(<-read)
}

// A dead watchdog is said once, in terms a user can look for, not once per key with
// the key's xapi reference: the restore's own ID, which every object it made carries.
func TestLostWatchdogIsSaidOnce(t *testing.T) {
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	p.watchdog = watchdog.FromWriter(deadWatchdog{})
	var closeErr error
	out := stderrOf(t, func() {
		restored := restoreOneDisk(t, p)
		if err := p.createVM(context.Background(), sbVM, restored, true); err != nil {
			t.Fatal(err)
		}
		closeErr = p.Close(context.Background())
	})
	if n := strings.Count(out, "xcpng: warning: the restore goes on"); n != 1 {
		t.Fatalf("said %d times:\n%s", n, out)
	}
	if strings.Contains(out, "OpaqueRef") {
		t.Errorf("a warning names an xapi reference:\n%s", out)
	}
	id := p.restoreID()
	if !strings.Contains(out, id) {
		t.Errorf("the warning does not name restore %s:\n%s", id, out)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.vdiDescs) != 1 || !strings.Contains(f.vdiDescs[0], id) {
		t.Errorf("VDI descriptions %q do not hold %s", f.vdiDescs, id)
	}
	for ref, m := range f.vmMarkers {
		if ref != "OpaqueRef:samename" && !strings.HasPrefix(m, id+"-") {
			t.Errorf("VM %s marked %q, not under %s", ref, m, id)
		}
	}
	if errors.Is(closeErr, os.ErrClosed) {
		t.Errorf("Close failed on the watchdog already reported: %v", closeErr)
	}
}

// Starting the watchdog says the restore ID once, with where to look, before the first
// create: if the watchdog is killed after Ctrl-C has ended the plugin, nothing is left
// to say it (#56).
func TestRestoreIDIsSaidWhenTheWatchdogStarts(t *testing.T) {
	f := &restoreXAPI{}
	createsBefore := -1
	old := startWatchdog
	startWatchdog = func(string, string, map[string]string) (*watchdog.Watchdog, error) {
		f.mu.Lock()
		createsBefore = len(f.vdiDescs)
		f.mu.Unlock()
		return watchdog.FromWriter(&recordWatchdog{}), nil
	}
	t.Cleanup(func() { startWatchdog = old })
	p := restoreExporter(t, f)
	p.watchdog = nil
	out := stderrOf(t, func() {
		restoreOneDisk(t, p)
		p.watch(vdiKey("OpaqueRef:b"))
	})
	if createsBefore != 0 {
		t.Fatalf("watchdog started after %d VDI.create calls", createsBefore)
	}
	said := "xcpng: restore " + p.restoreID() + ":"
	if n := strings.Count(out, said); n != 1 {
		t.Fatalf("%q said %d times:\n%s", said, n, out)
	}
	if !strings.Contains(out, "other_config:"+restoreMarkerKey) || strings.Contains(out, "warning") {
		t.Errorf("not where to look, or said as a warning:\n%s", out)
	}
}

// A key the watchdog refuses leaves it running: that key is named, the later ones are
// still listed.
func TestRefusedKeyKeepsTheWatchdog(t *testing.T) {
	p, rec := recordedExporter(t, &restoreXAPI{})
	out := stderrOf(t, func() {
		p.watch("vdi/" + strings.Repeat("x", 1<<20))
		p.watch(vdiKey("OpaqueRef:b"))
	})
	if p.watchdogLost || !strings.Contains(out, "key refused") {
		t.Fatalf("lost=%v, said:\n%s", p.watchdogLost, out)
	}
	if got := rec.pending(t); !slices.Equal(got, []string{"vdi/OpaqueRef:b"}) {
		t.Fatalf("listed %q", got)
	}
}

// A VDI xapi still holds when heldWait runs out stays listed, and its error says the
// watchdog tries again: without that it reads as a disk left on the pool (#78).
func TestHeldVDIErrorSaysTheWatchdogRetries(t *testing.T) {
	old := heldWait
	heldWait = 50 * time.Millisecond
	t.Cleanup(func() { heldWait = old })
	f := &restoreXAPI{}
	p, rec := recordedExporter(t, f)
	restored := restoreOneDisk(t, p)
	f.held = map[string]bool{"OpaqueRef:vdi0": true}
	err := p.destroyDisks(context.Background(), sbVM, restored)
	if err == nil || !strings.Contains(err.Error(), "VDI_IN_USE") || !strings.Contains(err.Error(), heldRetryHint) {
		t.Fatalf("error %v does not say the watchdog tries again", err)
	}
	if got := rec.pending(t); !slices.Contains(got, vdiKey("OpaqueRef:vdi0")) {
		t.Fatalf("listed %q, want the held VDI", got)
	}
}

// With the watchdog lost nothing tries again, so the error does not say it does.
func TestHeldVDIErrorWithoutWatchdog(t *testing.T) {
	old := heldWait
	heldWait = 50 * time.Millisecond
	t.Cleanup(func() { heldWait = old })
	f := &restoreXAPI{}
	p := restoreExporter(t, f)
	p.watchdog = watchdog.FromWriter(deadWatchdog{})
	var err error
	stderrOf(t, func() {
		restored := restoreOneDisk(t, p)
		f.held = map[string]bool{"OpaqueRef:vdi0": true}
		err = p.destroyDisks(context.Background(), sbVM, restored)
	})
	if !p.watchdogLost {
		t.Fatal("the watchdog was not lost")
	}
	if err == nil || strings.Contains(err.Error(), heldRetryHint) {
		t.Fatalf("error %v says a lost watchdog tries again", err)
	}
}
