package exporter

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// killed is what the test's labPause panics with: the restore stops there, as a killed
// plugin does, and none of its in-process cleanup runs after it.
type killed struct{}

// killAt runs createVM until it reaches phase and stops it there. It reports whether
// the phase was reached.
func killAt(t *testing.T, p *Exporter, phase string, restored map[string]restoredDisk) bool {
	t.Helper()
	return stopAt(t, phase, func() { _ = p.createVM(context.Background(), sbVM, restored, true) })
}

// stopAt runs restore until it reaches phase and stops it there. It reports whether the
// phase was reached.
func stopAt(t *testing.T, phase string, restore func()) (reached bool) {
	t.Helper()
	old := labPause
	labPause = func(at string) {
		if at == phase {
			panic(killed{})
		}
	}
	t.Cleanup(func() { labPause = old })
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(killed); !ok {
				panic(r)
			}
			reached = true
		}
	}()
	restore()
	return false
}

// leftovers lists the VMs and VDIs the fake still holds.
func (f *restoreXAPI) leftovers() (vms, vdis []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ref := range f.vmMarkers {
		if ref != "OpaqueRef:samename" && !slices.Contains(f.destroyed, ref) {
			vms = append(vms, ref)
		}
	}
	for ref := range f.vdiLabels {
		if !slices.Contains(f.destroyed, ref) {
			vdis = append(vdis, ref)
		}
	}
	return vms, vdis
}

func (f *restoreXAPI) marker(ref string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.vmMarkers[ref]
}

// findable reports whether the lost-watchdog warning leads the user to ref: a VM whose
// marker starts with the restore ID, or a VDI holding the ID in its name or description
// that no VM uses. A VDI attached to such a VM goes with it.
func (f *restoreXAPI) findable(id, ref string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	marked := func(vm string) bool {
		return strings.HasPrefix(f.vmMarkers[vm], id) && !slices.Contains(f.unmarked, vm+" "+restoreMarkerKey)
	}
	if _, ok := f.vmMarkers[ref]; ok {
		return marked(ref)
	}
	if vm, ok := f.vbds[ref]; ok {
		return marked(vm)
	}
	return strings.Contains(f.vdiLabels[ref], id) || strings.Contains(f.vdiDesc[ref], id)
}

// A restore can be killed at any point. With the watchdog alive, everything it made and
// has not kept must be listed with it; with the watchdog lost, findable as the warning
// says. What it has kept must not be listed: the watchdog would destroy a restored VM.
func TestKillAtPausePoints(t *testing.T) {
	for _, tc := range []struct {
		phase     string
		readiness string
		kept      bool
	}{
		{"vm-created", "", false},
		{"vbd-created", "", false},
		{"attached", "", false},
		{"secureboot", "", false},
		{"secureboot", "first_boot", false},
		{"kept", "", true},
		{"kept", "first_boot", true},
	} {
		for _, lost := range []bool{false, true} {
			name := tc.phase + "/" + tc.readiness
			if lost {
				name += "/lost"
			}
			t.Run(name, func(t *testing.T) {
				f := &restoreXAPI{sbReadiness: tc.readiness}
				p, rec := recordedExporter(t, f)
				if lost {
					p.watchdog, p.watchdogLost = nil, true
				}
				restored := restoreOneDisk(t, p)
				if !killAt(t, p, tc.phase, restored) {
					t.Fatalf("createVM never reached %s", tc.phase)
				}

				f.mu.Lock()
				lostID := f.lostID
				f.mu.Unlock()
				if len(lostID) != 0 {
					t.Fatalf("%q renamed out of the restore ID before a VBD held it", lostID)
				}

				vms, vdis := f.leftovers()
				if len(vms) != 1 || len(vdis) != 1 {
					t.Fatalf("left VMs %q and VDIs %q, want one of each", vms, vdis)
				}
				pending := rec.pending(t)
				if tc.kept {
					if !lost && len(pending) != 0 {
						t.Fatalf("kept, yet %q listed: the watchdog would destroy a restored VM", pending)
					}
					return
				}
				for _, ref := range append(vms, vdis...) {
					if lost && !f.findable(p.restoreID(), ref) {
						t.Errorf("%s: the lost-watchdog warning does not lead to it", ref)
					}
					// Until VM.create answers, the VM is listed by its marker.
					byLabel := vmLabelKey("web", f.marker(ref))
					if !lost && !slices.Contains(pending, vmKey(ref)) && !slices.Contains(pending, vdiKey(ref)) && !slices.Contains(pending, byLabel) {
						t.Errorf("%s: not listed with the watchdog (%q)", ref, pending)
					}
				}
			})
		}
	}
}

// The same, for a restore killed while its disk is being written: the one VDI it made is
// listed with the watchdog, by its label until VDI.create has answered, or findable.
func TestKillInsideRestoreDisk(t *testing.T) {
	for _, phase := range []string{"vdi-created", "vdi-renamed", "task-created", "vdi-imported"} {
		for _, lost := range []bool{false, true} {
			name := phase
			if lost {
				name += "/lost"
			}
			t.Run(name, func(t *testing.T) {
				f := &restoreXAPI{}
				p, rec := recordedExporter(t, f)
				if lost {
					p.watchdog, p.watchdogLost = nil, true
				}
				if !stopAt(t, phase, func() { restoreOneDisk(t, p) }) {
					t.Fatalf("restoreDisk never reached %s", phase)
				}

				vms, vdis := f.leftovers()
				if len(vms) != 0 || len(vdis) != 1 {
					t.Fatalf("left VMs %q and VDIs %q, want one VDI", vms, vdis)
				}
				ref := vdis[0]
				// Killed at task-created, the import leaves its task, never used, which
				// xapi would keep pending for a day: listed by its label, or named with
				// the restore ID. At the other pause points the task is gone or not made.
				left := f.tasksLeft()
				if phase != "task-created" && len(left) != 0 {
					t.Errorf("tasks %q left", left)
				}
				if phase == "task-created" {
					if len(left) != 1 {
						t.Fatalf("tasks %q left, want the import's", left)
					}
					label, ok := strings.CutPrefix(left[0], "plakar import_raw_vdi ")
					if lost && (!ok || !strings.HasPrefix(label, p.restoreID()+"-")) {
						t.Errorf("task %q: the lost-watchdog warning does not lead to it", left[0])
					}
					if pending := rec.pending(t); !lost && !slices.Contains(pending, taskLabelKey(label)) {
						t.Errorf("task %q: not listed with the watchdog (%q)", left[0], pending)
					}
				}
				if lost {
					if !f.findable(p.restoreID(), ref) {
						t.Errorf("%s: the lost-watchdog warning does not lead to it", ref)
					}
					return
				}
				f.mu.Lock()
				byLabel := vdiLabelKey("OpaqueRef:sr", f.vdiLabels[ref])
				f.mu.Unlock()
				if pending := rec.pending(t); !slices.Contains(pending, vdiKey(ref)) && !slices.Contains(pending, byLabel) {
					t.Errorf("%s: not listed with the watchdog (%q)", ref, pending)
				}
			})
		}
	}
}

// A restore killed while it writes a disk leaves the import's task: it is listed by its
// name for as long as the import runs, and named as an import, with the restore ID.
func TestImportTaskIsListedWhileItRuns(t *testing.T) {
	f := &restoreXAPI{}
	p, rec := recordedExporter(t, f)
	var during []string
	var names []string
	f.duringImport = func() {
		during = rec.pending(t)
		names = f.tasksLeft()
	}
	restoreOneDisk(t, p)
	prefix := "plakar import_raw_vdi " + p.restoreID() + "-"
	if len(names) != 1 || !strings.HasPrefix(names[0], prefix) {
		t.Fatalf("tasks during the import %q, want one named %s...", names, prefix)
	}
	key := taskLabelKey(strings.TrimPrefix(names[0], "plakar import_raw_vdi "))
	if !slices.Contains(during, key) {
		t.Errorf("listed %q during the import, want %s", during, key)
	}
	if left := f.tasksLeft(); len(left) != 0 {
		t.Errorf("tasks %q left after the import", left)
	}
	for _, key := range rec.pending(t) {
		if strings.HasPrefix(key, "task-label/") {
			t.Errorf("%s still listed after the import", key)
		}
	}
}

// A failed import's task stays listed: its destroy only marks a task still pending, and
// one whose request never reached xapi stays pending.
func TestFailedImportKeepsItsTaskListed(t *testing.T) {
	f := &restoreXAPI{failImport: true}
	p, rec := recordedExporter(t, f)
	if _, err := p.restoreDisk(context.Background(), "OpaqueRef:sr", false, "disk-0", diskRecord(make([]byte, 4096))); err == nil {
		t.Fatal("restoreDisk succeeded although the import failed")
	}
	f.mu.Lock()
	var name string
	for _, n := range f.tasks {
		name = n
	}
	f.mu.Unlock()
	label, _ := strings.CutPrefix(name, "plakar import_raw_vdi ")
	if pending := rec.pending(t); !slices.Equal(pending, []string{taskLabelKey(label)}) {
		t.Fatalf("listed %q, want only %s", pending, taskLabelKey(label))
	}
}
