// Package watchdog runs a child process that outlives a killed plugin and cleans up
// what the plugin left on the pool.
//
// Plakar starts plugins with exec.CommandContext, so when plakar is interrupted the
// plugin gets SIGKILL and none of its own cleanup runs. The watchdog is a child process
// that ignores interrupts and outlives the plugin: the plugin tells it over a pipe which
// objects it has made and not yet kept or destroyed, and if the pipe closes while some
// are still listed, the plugin died and the watchdog destroys them.
//
// Protocol, one line each: the connector configuration as JSON, then "+<key>" when an
// object is listed and "-<key>" once it is kept or destroyed. What a key names is up to
// the plugin.
package watchdog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// maxLine bounds a protocol line; the configuration is the longest.
const maxLine = 1 << 20

// maxKey leaves room in a line for the operation byte and the newline.
const maxKey = maxLine - 2

// CleanupTimeout bounds the watchdog's cleanup of everything still listed.
const CleanupTimeout = 10 * time.Minute

type Watchdog struct {
	name string
	mu   sync.Mutex
	w    io.WriteCloser
	done chan struct{}
}

// Start starts the watchdog child: this executable, run with arg as its first argument.
// name says which watchdog it is in errors. The configuration goes over the pipe, not
// the command line or the environment, because it holds the XAPI password.
func Start(name, arg string, config map[string]string) (*Watchdog, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	line, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	cmd := Command(exe, arg)
	cmd.Stderr = os.Stderr
	w, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	wd := &Watchdog{name: name, w: w, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(wd.done)
	}()
	if err := wd.send(string(line)); err != nil {
		_ = w.Close()
		return nil, err
	}
	return wd, nil
}

// FromWriter returns a Watchdog with no child process, writing the protocol to w, for
// tests that check what a plugin lists.
func FromWriter(w io.WriteCloser) *Watchdog {
	done := make(chan struct{})
	close(done)
	return &Watchdog{name: "watchdog", w: w, done: done}
}

// Command builds the command that starts the watchdog child, detached from the
// plugin's process group where the platform allows it.
func Command(exe string, args ...string) *exec.Cmd {
	cmd := exec.Command(exe, args...)
	detach(cmd)
	return cmd
}

func (wd *Watchdog) send(line string) error {
	wd.mu.Lock()
	defer wd.mu.Unlock()
	if _, err := io.WriteString(wd.w, line+"\n"); err != nil {
		return fmt.Errorf("%s: %w", wd.name, err)
	}
	return nil
}

// Add and Remove refuse a key holding a line break: it would end the line and make
// the rest a protocol line of its own, listing or unlisting what the key never named.
// They refuse a key too long for Read as well, which stops at such a line and would
// lose every unlisting after it.
func (wd *Watchdog) Add(key string) error    { return wd.sendKey('+', key) }
func (wd *Watchdog) Remove(key string) error { return wd.sendKey('-', key) }

func (wd *Watchdog) sendKey(op byte, key string) error {
	if strings.ContainsAny(key, "\r\n") {
		return fmt.Errorf("%s: key %q holds a line break", wd.name, key)
	}
	if len(key) > maxKey {
		return fmt.Errorf("%s: key of %d bytes is too long", wd.name, len(key))
	}
	return wd.send(string(op) + key)
}

// Close tells the watchdog the plugin is done. Objects still listed are destroyed by
// the watchdog, so this waits for it, within a bound.
func (wd *Watchdog) Close() error {
	wd.mu.Lock()
	err := wd.w.Close()
	wd.mu.Unlock()
	select {
	case <-wd.done:
	case <-time.After(CleanupTimeout):
		err = errors.Join(err, fmt.Errorf("%s did not exit", wd.name))
	}
	return err
}

// Read reads the protocol until end of input and returns the configuration and the
// keys still listed, in the order they were added.
func Read(r io.Reader) (map[string]string, []string, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), maxLine)

	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	var config map[string]string
	if err := json.Unmarshal(sc.Bytes(), &config); err != nil {
		return nil, nil, fmt.Errorf("configuration: %w", err)
	}

	var order []string
	listed := map[string]bool{}
	for sc.Scan() {
		line := sc.Text()
		if len(line) < 2 {
			continue
		}
		key := line[1:]
		switch line[0] {
		case '+':
			if !listed[key] {
				listed[key] = true
				order = append(order, key)
			}
		case '-':
			delete(listed, key)
		}
	}
	// A read error means the pipe is unusable, which is how a dead plugin looks too:
	// fall through and clean up what is listed.
	var pending []string
	for _, key := range order {
		if listed[key] {
			pending = append(pending, key)
		}
	}
	return config, pending, nil
}

// IgnoreSignals makes the watchdog outlive the interrupts that end plakar.
// SIGPIPE too: stderr may be a pipe whose reader died with plakar, and Go exits on
// SIGPIPE from a write to stderr, which would stop the cleanup half way.
func IgnoreSignals() {
	signal.Ignore(os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)
}

// Is reports whether the process was started as the watchdog selected by arg.
func Is(args []string, arg string) bool {
	return len(args) > 1 && strings.TrimSpace(args[1]) == arg
}
