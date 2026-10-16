package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"integration-xcpng/internal/xapi"
)

// snapshotXAPI answers the calls destroySnapshot makes, for one snapshot VM with two
// snapshot disks, and records every destroy call in order.
type snapshotXAPI struct {
	failVDI string // VDI ref whose VDI.destroy fails
	// down is how many requests are dropped without an answer, as during a toolstack
	// restart. Once one is dropped, the old session is invalid, as after a restart.
	down int
	// slave is how many calls answer HOST_IS_SLAVE, as after a master failover.
	slave int
	// hang makes every call wait until the client gives up, as with a host that
	// stopped answering without closing connections.
	hang bool
	// attached lists the snapshot VDIs that have an attached VBD, as during an export,
	// which plugs the VDI into dom0.
	attached map[string]bool
	// serverTime is what host.get_servertime answers; empty means one day after the
	// snapshot was taken.
	serverTime string
	// labelled makes VM.get_snapshots list the snapshot only once VM.snapshot has
	// created it, under the label it was given.
	labelled bool
	// snapshotHangs makes VM.snapshot create the snapshot, then never answer.
	snapshotHangs bool
	// snapshotCode makes VM.snapshot fail with this XAPI error and create nothing.
	snapshotCode string
	// inProgress is how many VM.get_current_operations answers still show the
	// snapshot being taken; the snapshot exists once that reaches 0.
	inProgress int
	// recordEarly makes the snapshot record exist while the operation still shows,
	// as in xapi, which creates it before copying the VBDs, VIFs and vTPMs.
	recordEarly bool

	mu        sync.Mutex
	calls     []string // "VDI.destroy <ref>", "VM.destroy <ref>"
	restarted bool
	label     string // what VM.snapshot was asked to name the snapshot
	created   bool
	opsPolls  int // VM.get_current_operations calls
}

func (f *snapshotXAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	if f.down > 0 {
		f.down--
		f.restarted = true
		f.mu.Unlock()
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
		return
	}
	if f.hang {
		f.mu.Unlock()
		<-r.Context().Done()
		return
	}
	if f.slave > 0 && req.Method != "session.login_with_password" {
		f.slave--
		f.restarted = true
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": "HOST_IS_SLAVE", "data": []string{"127.0.0.1"}},
		})
		return
	}
	session := "OpaqueRef:session"
	if f.restarted {
		session = "OpaqueRef:session-after-restart"
	}
	f.mu.Unlock()
	arg := func(i int) string {
		var s string
		if i < len(req.Params) {
			_ = json.Unmarshal(req.Params[i], &s)
		}
		return s
	}
	fail := func(code string) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": code, "data": []string{arg(1)}},
		})
	}
	if req.Method != "session.login_with_password" && arg(0) != session {
		fail("SESSION_INVALID")
		return
	}
	var result any
	switch req.Method {
	case "session.login_with_password":
		result = session
	case "VM.get_VBDs":
		result = []string{"OpaqueRef:vbd-a", "OpaqueRef:vbd-b", "OpaqueRef:vbd-cd"}
	case "VBD.get_record":
		switch arg(1) {
		case "OpaqueRef:vbd-a":
			result = xapi.VBDRecord{Type: "Disk", VDI: "OpaqueRef:vdi-a"}
		case "OpaqueRef:vbd-b":
			result = xapi.VBDRecord{Type: "Disk", VDI: "OpaqueRef:vdi-b"}
		default:
			result = xapi.VBDRecord{Type: "CD", VDI: xapi.NullRef, Empty: true}
		}
	case "VDI.get_is_a_snapshot":
		result = true
	case "VM.get_snapshots":
		f.mu.Lock()
		result = []string{"OpaqueRef:snap"}
		if f.labelled && !f.created {
			result = []string{}
		}
		f.mu.Unlock()
	case "VM.snapshot":
		if f.snapshotCode != "" {
			fail(f.snapshotCode)
			return
		}
		f.mu.Lock()
		f.label = arg(2)
		f.created = f.inProgress == 0 || f.recordEarly
		f.mu.Unlock()
		if f.snapshotHangs {
			<-r.Context().Done()
			return
		}
		result = "OpaqueRef:snap"
	case "VM.get_current_operations":
		ops := map[string]string{}
		f.mu.Lock()
		f.opsPolls++
		if f.inProgress > 0 {
			f.inProgress--
			ops["OpaqueRef:task"] = "snapshot"
			f.created = f.inProgress == 0 || f.recordEarly
		}
		f.mu.Unlock()
		result = ops
	case "VM.get_name_label":
		f.mu.Lock()
		result = f.label
		f.mu.Unlock()
	case "VM.get_by_uuid":
		if arg(1) != "vm-uuid" {
			fail("UUID_INVALID")
			return
		}
		result = "OpaqueRef:vm"
	case "VM.get_record":
		if arg(1) == "OpaqueRef:vm" {
			result = map[string]string{"uuid": "vm-uuid"}
			break
		}
		result = snapshotSummary{
			UUID:         "snap-uuid",
			OtherConfig:  map[string]string{snapshotMarker: "snap-uuid"},
			SnapshotTime: "20261001T00:00:00Z",
		}
	case "pool.get_all":
		result = []string{"OpaqueRef:pool"}
	case "pool.get_master":
		result = "OpaqueRef:master"
	case "host.get_servertime":
		result = f.serverTime
		if f.serverTime == "" {
			result = "20261002T00:00:00Z"
		}
	case "VDI.get_VBDs":
		// The snapshot VM's own VBD, and dom0's VBD while the VDI is attached there.
		vbds := []string{"OpaqueRef:snapvbd-" + arg(1)}
		if f.attached[arg(1)] {
			vbds = append(vbds, "OpaqueRef:dom0vbd-"+arg(1))
		}
		result = vbds
	case "VBD.get_currently_attached":
		result = strings.HasPrefix(arg(1), "OpaqueRef:dom0vbd-")
	case "VDI.destroy", "VM.destroy":
		f.mu.Lock()
		if f.inProgress > 0 {
			f.calls = append(f.calls, "while snapshotting")
		}
		f.calls = append(f.calls, req.Method+" "+arg(1))
		f.mu.Unlock()
		if req.Method == "VDI.destroy" && arg(1) == f.failVDI {
			fail("SR_BACKEND_FAILURE_1200")
			return
		}
		result = ""
	default:
		fail("MESSAGE_METHOD_UNKNOWN")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func TestDestroySnapshotDestroysVDIsBeforeVM(t *testing.T) {
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// A VDI that cannot be destroyed must leave the snapshot VM in place: it carries the
// marker the sweep looks for, and without it the VDI would be leaked for good.
func TestDestroySnapshotKeepsVMWhenAVDIFails(t *testing.T) {
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	err := p.destroySnapshot(context.Background(), "OpaqueRef:snap")
	if !xapi.IsCode(err, "SR_BACKEND_FAILURE_1200") {
		t.Fatalf("err = %v, want SR_BACKEND_FAILURE_1200", err)
	}
	// The other disk is still destroyed: one bad VDI does not leak the rest.
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q (no VM.destroy)", f.calls, want)
	}
}

// A toolstack restart right after the export used to make cleanup fail at once with
// "connection refused", leaking the snapshot and its VDIs. Cleanup must wait for XAPI
// to come back and log in again, since sessions do not survive a restart.
func TestDestroySnapshotWaitsForXAPIRestart(t *testing.T) {
	fastRetry(t)
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.down = 3
	f.mu.Unlock()
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// The wait for XAPI is bounded by the caller's context.
func TestDestroySnapshotGivesUpWhenXAPIStaysDown(t *testing.T) {
	fastRetry(t)
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.down = 1 << 30
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := p.destroySnapshot(ctx, "OpaqueRef:snap"); err == nil {
		t.Fatal("destroySnapshot succeeded with XAPI down")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("gave up after %s, want about 500ms", d)
	}
}

func fastRetry(t *testing.T) {
	t.Helper()
	oldDelay, oldRetry := xapiDownDelay, xapiDownRetry
	xapiDownDelay = 10 * time.Millisecond
	t.Cleanup(func() { xapiDownDelay, xapiDownRetry = oldDelay, oldRetry })
}

// After a master failover the old master answers HOST_IS_SLAVE; cleanup must log in
// again, which finds the new master.
func TestDestroySnapshotFollowsNewMaster(t *testing.T) {
	fastRetry(t)
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.slave = 1
	f.mu.Unlock()
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// A call that never returns must not hold cleanup past its retry bound, even when the
// caller's context has no deadline (the sweep, the early error paths).
func TestDestroySnapshotBoundsAHangingCall(t *testing.T) {
	fastRetry(t)
	xapiDownRetry = 300 * time.Millisecond
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	f.mu.Lock()
	f.hang = true
	f.mu.Unlock()
	start := time.Now()
	if err := p.destroySnapshot(context.Background(), "OpaqueRef:snap"); err == nil {
		t.Fatal("destroySnapshot succeeded with xapi hanging")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("gave up after %s, want about 300ms", d)
	}
}

// A complete backup is kept when only the snapshot cleanup fails: the snapshot is
// marked, so the sweep removes it, and the user is told which one it is.
func TestFinishSnapshotWarnsWhenOnlyCleanupFails(t *testing.T) {
	var out strings.Builder
	warnings = &out
	t.Cleanup(func() { warnings = os.Stderr })
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	p.staleAge = defaultStaleAge
	if err := p.finishSnapshot(context.Background(), nil, nil, "OpaqueRef:snap", "snap-uuid"); err != nil {
		t.Fatalf("complete backup failed on cleanup: %v", err)
	}
	for _, want := range []string{"snap-uuid", "SR_BACKEND_FAILURE_1200", "24h0m0s"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("warning %q does not mention %q", out.String(), want)
		}
	}
}

// With the sweep disabled nothing will remove the snapshot: the warning says so.
func TestFinishSnapshotWarnsSweepDisabled(t *testing.T) {
	var out strings.Builder
	warnings = &out
	t.Cleanup(func() { warnings = os.Stderr })
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	if err := p.finishSnapshot(context.Background(), nil, nil, "OpaqueRef:snap", "snap-uuid"); err != nil {
		t.Fatalf("complete backup failed on cleanup: %v", err)
	}
	if !strings.Contains(out.String(), "stale_snapshot_age is 0") {
		t.Errorf("warning %q does not say the sweep is disabled", out.String())
	}
}

// A backup that already failed still fails, and reports the cleanup error with it.
func TestFinishSnapshotKeepsBackupError(t *testing.T) {
	var out strings.Builder
	warnings = &out
	t.Cleanup(func() { warnings = os.Stderr })
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	backupErr := errors.New("export failed")
	err := p.finishSnapshot(context.Background(), backupErr, nil, "OpaqueRef:snap", "snap-uuid")
	if !errors.Is(err, backupErr) || !strings.Contains(fmt.Sprint(err), "SR_BACKEND_FAILURE_1200") {
		t.Errorf("err = %v, want the backup error joined with the cleanup error", err)
	}
	if out.Len() != 0 {
		t.Errorf("unexpected warning %q", out.String())
	}
}

// A disk that Plakar failed to store makes the backup incomplete: the cleanup error
// still fails it, as before.
func TestFinishSnapshotFailsWhenAStreamFailed(t *testing.T) {
	var out strings.Builder
	warnings = &out
	t.Cleanup(func() { warnings = os.Stderr })
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a"}
	p := dialFake(t, f)
	err := p.finishSnapshot(context.Background(), nil, errResultsClosed, "OpaqueRef:snap", "snap-uuid")
	if !strings.Contains(fmt.Sprint(err), "SR_BACKEND_FAILURE_1200") {
		t.Errorf("err = %v, want the cleanup error", err)
	}
	if out.Len() != 0 {
		t.Errorf("unexpected warning %q", out.String())
	}
}

// The sweep destroys a marked snapshot older than stale_snapshot_age.
func TestSweepDestroysStaleSnapshot(t *testing.T) {
	f := &snapshotXAPI{}
	p := dialFake(t, f)
	p.staleAge = time.Hour
	if err := p.destroyStaleSnapshots(context.Background(), "OpaqueRef:vm"); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// A backup that runs longer than stale_snapshot_age is still exporting its snapshot.
// Another backup of the same VM must leave it alone: xapi refuses VDI.destroy on the
// attached disk, and the sweep used to retry that for two minutes, then fail the
// second backup.
func TestSweepSkipsSnapshotWithAttachedDisk(t *testing.T) {
	var buf strings.Builder
	old := warnings
	warnings = &buf
	t.Cleanup(func() { warnings = old })

	f := &snapshotXAPI{attached: map[string]bool{"OpaqueRef:vdi-b": true}}
	p := dialFake(t, f)
	p.staleAge = time.Hour
	if err := p.destroyStaleSnapshots(context.Background(), "OpaqueRef:vm"); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 0 {
		t.Errorf("calls = %q, want none", f.calls)
	}
	if !strings.Contains(buf.String(), "snap-uuid") {
		t.Errorf("warning = %q, want one naming the snapshot", buf.String())
	}
}

// The sweep measures a snapshot's age on the pool's clock, which stamped snapshot_time.
// Here the plakar host's clock is far ahead of the pool's: by the pool's clock the
// snapshot is an hour old, and a backup may still be reading it.
func TestSweepUsesPoolClock(t *testing.T) {
	f := &snapshotXAPI{serverTime: "20261001T01:00:00Z"}
	p := dialFake(t, f)
	p.staleAge = 24 * time.Hour
	if err := p.destroyStaleSnapshots(context.Background(), "OpaqueRef:vm"); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 0 {
		t.Errorf("calls = %q, want none", f.calls)
	}
}

// A snapshot the sweep cannot remove used to fail the backup, and with vm_tag every VM
// after it. The sweep only tidies up what an earlier backup left: the backup goes on
// and the user is told. The fake refuses VM.snapshot, so failing there shows the backup
// got past the sweep.
func TestSweepErrorDoesNotFailBackup(t *testing.T) {
	var out strings.Builder
	warnings = &out
	t.Cleanup(func() { warnings = os.Stderr })
	f := &snapshotXAPI{failVDI: "OpaqueRef:vdi-a", snapshotCode: "VM_BAD_POWER_STATE"}
	p := dialFake(t, f)
	p.staleAge = time.Hour
	p.vtpm = vtpmSkip
	fakeWatchdog(p)
	err := p.backupVM(context.Background(), "OpaqueRef:vm", nil, nil)
	if !xapi.IsCode(err, "VM_BAD_POWER_STATE") || xapi.IsCode(err, "SR_BACKEND_FAILURE_1200") {
		t.Fatalf("err = %v, want the backup to reach VM.snapshot", err)
	}
	for _, want := range []string{"vm-uuid", "snap-uuid", "SR_BACKEND_FAILURE_1200"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("warning %q does not mention %q", out.String(), want)
		}
	}
}

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

func fakeWatchdog(p *Importer) *recordWatchdog {
	rec := &recordWatchdog{}
	done := make(chan struct{})
	close(done)
	p.watchdog = &watchdog{w: rec, done: done}
	return rec
}

func fastSnapshotGiveUp(t *testing.T) {
	t.Helper()
	oldTimeout, oldDelay := snapshotTimeout, labelPollDelay
	snapshotTimeout = 200 * time.Millisecond
	labelPollDelay = 10 * time.Millisecond
	t.Cleanup(func() { snapshotTimeout, labelPollDelay = oldTimeout, oldDelay })
}

// xapi finishes a snapshot when the caller has stopped waiting, and the snapshot used
// to be marked only once the call returned, so neither the watchdog nor the sweep ever
// removed it. The backup must find it by its label and destroy it.
func TestSnapshotGivenUpOnIsDestroyed(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true, snapshotHangs: true}
	p := dialFake(t, f)
	p.vtpm = vtpmSkip
	rec := fakeWatchdog(p)
	err := p.backupVM(context.Background(), "OpaqueRef:vm", nil, nil)
	if err == nil {
		t.Fatal("backup succeeded without a snapshot")
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
	// Listed with the watchdog before the call, and no longer once it is destroyed.
	key := "vm-uuid/" + f.label
	if !slices.Equal(rec.lines, []string{"+" + key, "-" + key}) {
		t.Errorf("watchdog lines = %q, want +%s then -%s", rec.lines, key, key)
	}
}

// xapi may still be taking the snapshot when the backup looks for it: the lookup waits
// while the VM shows a snapshot operation.
func TestSnapshotStillInProgressIsAwaited(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true, snapshotHangs: true, inProgress: 3}
	p := dialFake(t, f)
	p.vtpm = vtpmSkip
	fakeWatchdog(p)
	if err := p.backupVM(context.Background(), "OpaqueRef:vm", nil, nil); err == nil {
		t.Fatal("backup succeeded without a snapshot")
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// xapi names the snapshot record before it has copied the VBDs, VIFs and vTPMs
// (xapi_vm_clone.ml, clone): the lookup must leave it alone until the operation ends.
func TestSnapshotRecordIsLeftAloneUntilFinished(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true, snapshotHangs: true, inProgress: 3, recordEarly: true}
	p := dialFake(t, f)
	p.vtpm = vtpmSkip
	fakeWatchdog(p)
	if err := p.backupVM(context.Background(), "OpaqueRef:vm", nil, nil); err == nil {
		t.Fatal("backup succeeded without a snapshot")
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// An error xapi returned means it made no snapshot: nothing to look for.
func TestSnapshotRefusedByXAPIIsNotLookedFor(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true, snapshotCode: "VM_BAD_POWER_STATE"}
	p := dialFake(t, f)
	p.vtpm = vtpmSkip
	rec := fakeWatchdog(p)
	err := p.backupVM(context.Background(), "OpaqueRef:vm", nil, nil)
	if !xapi.IsCode(err, "VM_BAD_POWER_STATE") {
		t.Fatalf("err = %v, want VM_BAD_POWER_STATE", err)
	}
	if f.opsPolls != 0 || len(f.calls) != 0 {
		t.Errorf("looked for a snapshot xapi refused to take: %d polls, calls %q", f.opsPolls, f.calls)
	}
	if len(rec.lines) != 2 || rec.lines[1] != "-"+rec.lines[0][1:] {
		t.Errorf("watchdog lines = %q, want the label added then removed", rec.lines)
	}
}

// A killed plugin leaves the label listed; the watchdog finds the snapshot by it.
func TestWatchdogDestroysSnapshotByLabel(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true, created: true, label: "plakar-20261016T000000Z-0123abcd"}
	p := dialFake(t, f)
	if err := p.destroyListed(context.Background(), []string{"vm-uuid/" + f.label}); err != nil {
		t.Fatal(err)
	}
	want := []string{"VDI.destroy OpaqueRef:vdi-a", "VDI.destroy OpaqueRef:vdi-b", "VM.destroy OpaqueRef:snap"}
	if !slices.Equal(f.calls, want) {
		t.Errorf("calls = %q, want %q", f.calls, want)
	}
}

// The plugin was killed before xapi made the snapshot, or the VM is gone: nothing to do.
func TestWatchdogLabelWithoutSnapshot(t *testing.T) {
	fastSnapshotGiveUp(t)
	f := &snapshotXAPI{labelled: true}
	p := dialFake(t, f)
	keys := []string{"vm-uuid/plakar-20261016T000000Z-0123abcd", "gone-uuid/plakar-20261016T000000Z-0123abcd"}
	if err := p.destroyListed(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 0 {
		t.Errorf("calls = %q, want none", f.calls)
	}
}
