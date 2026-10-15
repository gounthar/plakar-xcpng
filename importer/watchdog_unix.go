//go:build unix

package importer

import (
	"os/exec"
	"syscall"
)

// detach starts the watchdog in a session of its own, so a signal sent to plakar's
// process group, SIGKILL included, does not reach it. A session rather than only a
// process group also drops the controlling terminal. plakar daemonises `plakar cached`
// the same way.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
