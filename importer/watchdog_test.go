package importer

import "testing"

func TestIsWatchdog(t *testing.T) {
	if !IsWatchdog([]string{"xcpngImporter", WatchdogArg}) {
		t.Error("watchdog argument not recognised")
	}
	if IsWatchdog([]string{"xcpngImporter"}) || IsWatchdog([]string{"xcpngImporter", "-x"}) {
		t.Error("plugin invocation taken for the watchdog")
	}
}
