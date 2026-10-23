//go:build !labhook

package exporter

// labPause does nothing outside a lab build (see labpause_on.go). It is a variable so
// that a test can stop a restore at a pause point.
var labPause = func(string) {}
