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

// labPause stops at phase while pause-<phase> exists in labPauseDir. On arrival it
// creates at-<phase>, which the script waits for before it interrupts the restore.
func labPause(phase string) {
	pause := filepath.Join(labPauseDir, "pause-"+phase)
	if _, err := os.Stat(pause); err != nil {
		return
	}
	if err := os.WriteFile(filepath.Join(labPauseDir, "at-"+phase), nil, 0o644); err != nil {
		warnf("lab pause %s: %s", phase, err)
	}
	for {
		if _, err := os.Stat(pause); errors.Is(err, fs.ErrNotExist) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
