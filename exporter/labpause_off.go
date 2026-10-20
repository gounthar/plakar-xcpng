//go:build !labhook

package exporter

// labPause does nothing outside a lab build (see labpause_on.go).
func labPause(string) {}
