package exporter

import (
	"context"
	"slices"
	"testing"
)

var orphansDisks = map[string]restoredDisk{
	"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
	"/vm/b": {ref: "OpaqueRef:b", size: 8192, kept: 8192},
}

// wantDisksDestroyed fails unless both restored disks of sizesVM were destroyed.
func wantDisksDestroyed(t *testing.T, f *destroyRecorder) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	got := slices.Sorted(slices.Values(f.destroyed))
	if !slices.Equal(got, []string{"OpaqueRef:a", "OpaqueRef:b"}) {
		t.Fatalf("destroyed %v, want OpaqueRef:a and OpaqueRef:b", f.destroyed)
	}
}

func TestCreateVMFailureDestroysDisks(t *testing.T) {
	f := &destroyRecorder{} // VM.create fails
	p := sizesExporter(t, f, options{})

	if err := p.createVM(context.Background(), sizesVM, orphansDisks, true); err == nil {
		t.Fatal("createVM succeeded although VM.create failed")
	}
	wantDisksDestroyed(t, f)
}

func TestAttachFailureDestroysVMAndDisks(t *testing.T) {
	f := &destroyRecorder{vmRef: "OpaqueRef:vm"} // VBD.create fails
	p := sizesExporter(t, f, options{})

	if err := p.createVM(context.Background(), sizesVM, orphansDisks, true); err == nil {
		t.Fatal("createVM succeeded although VBD.create failed")
	}
	if !slices.Equal(f.vmDestroyed, []string{"OpaqueRef:vm"}) {
		t.Fatalf("destroyed VMs %v, want OpaqueRef:vm", f.vmDestroyed)
	}
	// VM.destroy removes the VBDs, never the VDIs (xapi_vm_helpers.ml, destroy).
	wantDisksDestroyed(t, f)
}

func TestCheckDiskSizesDestroysDisksWhenInterrupted(t *testing.T) {
	f := &destroyRecorder{}
	p := sizesExporter(t, f, options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	short := map[string]restoredDisk{
		"/vm/a": {ref: "OpaqueRef:a", size: 4096, kept: 4096},
		"/vm/b": {ref: "OpaqueRef:b", size: 1000, kept: 1000},
	}
	if err := p.checkDiskSizes(ctx, sizesVM, short, true); err == nil {
		t.Fatal("short disk not refused")
	}
	wantDisksDestroyed(t, f)
}
