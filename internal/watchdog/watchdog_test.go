package watchdog

import (
	"slices"
	"strings"
	"testing"
)

func TestRead(t *testing.T) {
	cfgLine := `{"location":"xcpng://host","password":"p"}` + "\n"
	tests := []struct {
		name    string
		input   string
		pending []string
	}{
		{"clean exit", cfgLine + "+a\n-a\n", nil},
		{"killed mid-run", cfgLine + "+a\n", []string{"a"}},
		{"several objects, last one killed", cfgLine + "+a\n-a\n+b\n+c\n-b\n", []string{"c"}},
		{"deferred to Close, killed", cfgLine + "+a\n+b\n", []string{"a", "b"}},
		{"duplicate add listed once", cfgLine + "+a\n+a\n", []string{"a"}},
		{"truncated last line still counts", cfgLine + "+a\n+b", []string{"a", "b"}},
		{"nothing listed", cfgLine, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, pending, err := Read(strings.NewReader(tt.input))
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

func TestReadEmpty(t *testing.T) {
	config, pending, err := Read(strings.NewReader(""))
	if err != nil || config != nil || pending != nil {
		t.Errorf("got %v %v %v", config, pending, err)
	}
}

func TestReadBadConfig(t *testing.T) {
	if _, _, err := Read(strings.NewReader("not json\n+a\n")); err == nil {
		t.Error("expected an error")
	}
}

func TestIs(t *testing.T) {
	const arg = "--some-watchdog"
	if !Is([]string{"plugin", arg}, arg) {
		t.Error("watchdog argument not recognised")
	}
	if Is([]string{"plugin"}, arg) || Is([]string{"plugin", "-x"}, arg) {
		t.Error("plugin invocation taken for the watchdog")
	}
}

func TestKeyWithLineBreakIsRefused(t *testing.T) {
	var b strings.Builder
	wd := FromWriter(nopCloser{&b})
	for _, key := range []string{"a\n+b", "a\r", "\n"} {
		if err := wd.Add(key); err == nil {
			t.Errorf("Add(%q) accepted", key)
		}
		if err := wd.Remove(key); err == nil {
			t.Errorf("Remove(%q) accepted", key)
		}
	}
	if b.Len() != 0 {
		t.Fatalf("wrote %q", b.String())
	}
}

// Read stops at a line it cannot hold, and every unlisting after it would be lost.
func TestKeyTooLongForReadIsRefused(t *testing.T) {
	var b strings.Builder
	wd := FromWriter(nopCloser{&b})
	if err := wd.Add(strings.Repeat("k", maxKey+1)); err == nil {
		t.Fatal("a key too long for Read was accepted")
	}
	longest := strings.Repeat("k", maxKey)
	for _, step := range []func(string) error{wd.Add, wd.Remove} {
		if err := step(longest); err != nil {
			t.Fatal(err)
		}
	}
	if err := wd.Add("a"); err != nil {
		t.Fatal(err)
	}
	_, pending, err := Read(strings.NewReader("{}\n" + b.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pending, []string{"a"}) {
		t.Fatalf("pending = %.40q, want [a]: Read lost lines after the longest key", pending)
	}
}

type nopCloser struct{ *strings.Builder }

func (nopCloser) Close() error { return nil }
