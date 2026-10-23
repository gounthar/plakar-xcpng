//go:build labhook

package exporter

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// labPauseDir holds the files that pause a lab build. Only a build with the labhook tag
// has it: a lab script interrupts a restore at an exact step instead of racing it.
var labPauseDir = "/var/tmp/plakar-xcpng-lab"

// labPause is a variable so that a test can stop a restore at a pause point, as the
// lab script does by killing the plugin.
var labPause = pauseAt

// pauseAt stops at phase while pause-<phase> exists in labPauseDir. On arrival it
// creates at-<phase>, which the script waits for before it interrupts the restore, and
// removes it when it goes on, so a later restore cannot be taken for one already there.
// A killed plugin leaves it: the script clears both files before each restore.
func pauseAt(phase string) {
	pause := filepath.Join(labPauseDir, "pause-"+phase)
	if _, err := os.Stat(pause); err != nil {
		return
	}
	at := filepath.Join(labPauseDir, "at-"+phase)
	if err := os.WriteFile(at, nil, 0o644); err != nil {
		warnf("lab pause %s: %s", phase, err)
	}
	for {
		if _, err := os.Stat(pause); errors.Is(err, fs.ErrNotExist) {
			if err := os.Remove(at); err != nil && !errors.Is(err, fs.ErrNotExist) {
				warnf("lab pause %s: %s", phase, err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
