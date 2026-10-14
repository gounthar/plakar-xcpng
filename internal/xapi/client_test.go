package xapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestHandlerErrorsDoNotLeakSession(t *testing.T) {
	c := &Client{base: "https://127.0.0.1:1", http: &http.Client{}, session: "OpaqueRef:secret-session"}

	_, err := c.ExportRawVDI(context.Background(), "OpaqueRef:vdi", 4)
	if err == nil || strings.Contains(err.Error(), "secret-session") {
		t.Fatalf("export error leaks the session or is nil: %v", err)
	}

	err = c.ImportRawVDI(context.Background(), "OpaqueRef:vdi", 4, strings.NewReader("data"))
	if err == nil || strings.Contains(err.Error(), "secret-session") {
		t.Fatalf("import error leaks the session or is nil: %v", err)
	}
}

func TestParseDiskFilename(t *testing.T) {
	for _, tc := range []struct {
		in   string
		uuid string
		ok   bool
	}{
		{DiskFilename("0", "38717474-ae12-4c36-ada2-82a7b047dea0"), "38717474-ae12-4c36-ada2-82a7b047dea0", true},
		{"vm.json", "", false},
		{"0.raw", "", false},
		{"0-.raw", "", false},
	} {
		uuid, ok := ParseDiskFilename(tc.in)
		if uuid != tc.uuid || ok != tc.ok {
			t.Errorf("ParseDiskFilename(%q) = %q, %v; want %q, %v", tc.in, uuid, ok, tc.uuid, tc.ok)
		}
	}
}
