package importer

import (
	"slices"
	"strings"
	"testing"
)

func TestReadWatchdog(t *testing.T) {
	cfgLine := `{"location":"xcpng://host","password":"p"}` + "\n"
	tests := []struct {
		name    string
		input   string
		pending []string
	}{
		{"clean exit", cfgLine + "+a\n-a\n", nil},
		{"killed mid-backup", cfgLine + "+a\n", []string{"a"}},
		{"several VMs, last one killed", cfgLine + "+a\n-a\n+b\n+c\n-b\n", []string{"c"}},
		{"deferred to Close, killed", cfgLine + "+a\n+b\n", []string{"a", "b"}},
		{"duplicate add listed once", cfgLine + "+a\n+a\n", []string{"a"}},
		{"truncated last line still counts", cfgLine + "+a\n+b", []string{"a", "b"}},
		{"no snapshot taken", cfgLine, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, pending, err := readWatchdog(strings.NewReader(tt.input))
			if err != nil {
				t.Fatal(err)
			}
			if config["location"] != "xcpng://host" || config["password"] != "p" {
				t.Errorf("config = %v", config)
			}
			if !slices.Equal(pending, tt.pending) {
				t.Errorf("pending = %v, want %v", pending, tt.pending)
			}
		})
	}
}

func TestReadWatchdogEmpty(t *testing.T) {
	config, pending, err := readWatchdog(strings.NewReader(""))
	if err != nil || config != nil || pending != nil {
		t.Errorf("got %v %v %v", config, pending, err)
	}
}

func TestReadWatchdogBadConfig(t *testing.T) {
	if _, _, err := readWatchdog(strings.NewReader("not json\n+a\n")); err == nil {
		t.Error("expected an error")
	}
}

func TestIsWatchdog(t *testing.T) {
	if !IsWatchdog([]string{"xcpngImporter", WatchdogArg}) {
		t.Error("watchdog argument not recognised")
	}
	if IsWatchdog([]string{"xcpngImporter"}) || IsWatchdog([]string{"xcpngImporter", "-x"}) {
		t.Error("plugin invocation taken for the watchdog")
	}
}
