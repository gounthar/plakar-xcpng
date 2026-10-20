//go:build !unix

package watchdog

import "os/exec"

// detach does nothing where there are no Unix sessions.
func detach(*exec.Cmd) {}
