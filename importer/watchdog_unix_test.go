//go:build unix

package importer

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The helpers below run inside the test binary itself, selected by an environment
// variable: a stand-in for the plugin, which starts a stand-in for the watchdog with
// watchdogCommand, and the watchdog stand-in, which waits for its pipe to close and then
// leaves a marker file, the way the real one destroys what is still listed.
const helperEnv = "XCPNG_WATCHDOG_TEST_HELPER"

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "plugin":
		os.Exit(helperPlugin())
	case "watchdog":
		os.Exit(helperWatchdog())
	case "watchdog-stderr":
		os.Exit(helperWatchdogStderr())
	}
	os.Exit(m.Run())
}

func helperPlugin() int {
	cmd := watchdogCommand(os.Args[0])
	cmd.Env = append(os.Environ(), helperEnv+"=watchdog")
	cmd.Stderr = os.Stderr
	w, err := cmd.StdinPipe()
	if err != nil {
		return 2
	}
	if err := cmd.Start(); err != nil {
		return 2
	}
	_, _ = io.WriteString(w, "ready\n")
	_, _ = os.Stdout.WriteString("started\n")
	// Sleeping rather than select{}: a blocked-forever main goroutine can be ended by
	// the runtime's deadlock check, and an early exit would close the pipe and let the
	// watchdog stand-in pass without surviving anything.
	for {
		time.Sleep(time.Hour) // until killed
	}
}

func helperWatchdog() int {
	_, _ = io.Copy(io.Discard, os.Stdin)
	if err := os.WriteFile(os.Getenv("XCPNG_WATCHDOG_TEST_MARKER"), []byte("done"), 0o600); err != nil {
		return 1
	}
	return 0
}

// helperWatchdogStderr sets up signals as the watchdog does, waits for its pipe to close,
// writes to stderr as the watchdog does when it reports, then leaves the marker.
func helperWatchdogStderr() int {
	ignoreWatchdogSignals()
	_, _ = io.Copy(io.Discard, os.Stdin)
	_, _ = os.Stderr.WriteString("xcpng: cleanup of snapshot x: retrying\n")
	return helperWatchdog()
}

// When plakar's output goes through a pipe (`plakar ... 2>&1 | tee log`), the reader can
// die with plakar while the watchdog lives on. Its next stderr write must fail, not kill
// it with SIGPIPE before it has cleaned up.
func TestWatchdogSurvivesDeadStderrReader(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), helperEnv+"=watchdog-stderr", "XCPNG_WATCHDOG_TEST_MARKER="+marker)
	cmd.Stderr = w
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	_ = r.Close() // the reader is gone
	_ = stdin.Close()

	err = cmd.Wait()
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the watchdog died writing to a dead stderr: %v", err)
	}
}

// A SIGKILL to the plugin's whole process group, as `kill -9 -<pgid>` from a shell or a
// tool does, must not reach the watchdog, or the snapshot it guards leaks.
func TestWatchdogSurvivesProcessGroupKill(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker")

	plugin := exec.Command(os.Args[0])
	plugin.Env = append(os.Environ(), helperEnv+"=plugin", "XCPNG_WATCHDOG_TEST_MARKER="+marker)
	plugin.Stderr = os.Stderr
	// Its own group, so killing that group does not kill this test.
	plugin.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := plugin.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Start(); err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "started\n" {
		_ = plugin.Process.Kill()
		t.Fatalf("plugin stand-in: %q %v", line, err)
	}

	if err := syscall.Kill(-plugin.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = plugin.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the watchdog died with the plugin's process group")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
