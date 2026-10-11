package importer

import (
	"bufio"
	"context"
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

	"integration-xcpng/internal/xapi"
)

// WatchdogArg, as the first argument, makes the importer binary run as the snapshot
// watchdog instead of as a plugin.
//
// Plakar starts plugins with exec.CommandContext, so when plakar is interrupted the
// plugin gets SIGKILL and none of its own cleanup runs. The watchdog is a child process
// that ignores interrupts and outlives the plugin: the plugin tells it over a pipe which
// snapshots exist, and if the pipe closes while some are still listed, the plugin died
// and the watchdog destroys them.
//
// Protocol, one line each: the connector configuration as JSON, then "+<snapshot uuid>"
// when a snapshot is taken and "-<snapshot uuid>" once it is destroyed.
const WatchdogArg = "--xcpng-snapshot-watchdog"

// maxWatchdogLine bounds a protocol line; the configuration is the longest.
const maxWatchdogLine = 1 << 20

// watchdogCleanupTimeout bounds the watchdog's cleanup of all remaining snapshots.
const watchdogCleanupTimeout = 10 * time.Minute

type watchdog struct {
	mu   sync.Mutex
	w    io.WriteCloser
	done chan struct{}
}

// startWatchdog starts the watchdog child. The configuration goes over the pipe, not the
// command line or the environment, because it holds the XAPI password.
func startWatchdog(config map[string]string) (*watchdog, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("snapshot watchdog: %w", err)
	}
	line, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("snapshot watchdog: %w", err)
	}

	cmd := exec.Command(exe, WatchdogArg)
	cmd.Stderr = os.Stderr
	w, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("snapshot watchdog: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("snapshot watchdog: %w", err)
	}

	wd := &watchdog{w: w, done: make(chan struct{})}
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

func (wd *watchdog) send(line string) error {
	wd.mu.Lock()
	defer wd.mu.Unlock()
	if _, err := io.WriteString(wd.w, line+"\n"); err != nil {
		return fmt.Errorf("snapshot watchdog: %w", err)
	}
	return nil
}

func (wd *watchdog) add(snapUUID string) error    { return wd.send("+" + snapUUID) }
func (wd *watchdog) remove(snapUUID string) error { return wd.send("-" + snapUUID) }

// close tells the watchdog the plugin is done. Snapshots still listed are destroyed by
// the watchdog, so this waits for it, within a bound.
func (wd *watchdog) close() error {
	wd.mu.Lock()
	err := wd.w.Close()
	wd.mu.Unlock()
	select {
	case <-wd.done:
	case <-time.After(watchdogCleanupTimeout):
		err = errors.Join(err, errors.New("snapshot watchdog did not exit"))
	}
	return err
}

// readWatchdog reads the protocol until end of input and returns the configuration and
// the snapshots still listed, in the order they were added.
func readWatchdog(r io.Reader) (map[string]string, []string, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), maxWatchdogLine)

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
		uuid := line[1:]
		switch line[0] {
		case '+':
			if !listed[uuid] {
				listed[uuid] = true
				order = append(order, uuid)
			}
		case '-':
			delete(listed, uuid)
		}
	}
	// A read error means the pipe is unusable, which is how a dead plugin looks too:
	// fall through and clean up what is listed.
	var pending []string
	for _, uuid := range order {
		if listed[uuid] {
			pending = append(pending, uuid)
		}
	}
	return config, pending, nil
}

// RunWatchdog is the watchdog child's main. It returns the process exit status.
func RunWatchdog(stdin io.Reader) int {
	signal.Ignore(os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	config, pending, err := readWatchdog(stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: snapshot watchdog: %s\n", err)
		return 1
	}
	if len(pending) == 0 {
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), watchdogCleanupTimeout)
	defer cancel()
	if err := destroyLeftovers(ctx, config, pending); err != nil {
		fmt.Fprintf(os.Stderr, "xcpng: snapshot watchdog: %s\n", err)
		return 1
	}
	return 0
}

func destroyLeftovers(ctx context.Context, config map[string]string, uuids []string) error {
	cfg, err := xapi.ParseConfig(config)
	if err != nil {
		return err
	}
	client, err := xapi.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	p := &Importer{cfg: cfg, client: client}

	var errs []error
	for _, uuid := range uuids {
		var ref string
		if err := client.Call(ctx, &ref, "VM.get_by_uuid", uuid); err != nil {
			if xapi.IsCode(err, "UUID_INVALID") {
				continue // already gone
			}
			errs = append(errs, fmt.Errorf("snapshot %s: %w", uuid, err))
			continue
		}
		// Only a snapshot this connector marked as itself.
		var s snapshotSummary
		if err := client.Call(ctx, &s, "VM.get_record", ref); err != nil {
			errs = append(errs, fmt.Errorf("snapshot %s: %w", uuid, err))
			continue
		}
		if s.UUID != uuid || s.OtherConfig[snapshotMarker] != uuid {
			continue
		}
		if err := p.destroySnapshot(ctx, ref); err != nil {
			errs = append(errs, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "xcpng: destroyed snapshot %s left by an interrupted backup\n", uuid)
	}
	errs = append(errs, client.Close(ctx))
	return errors.Join(errs...)
}

// IsWatchdog reports whether the process was started as the snapshot watchdog.
func IsWatchdog(args []string) bool {
	return len(args) > 1 && strings.TrimSpace(args[1]) == WatchdogArg
}
