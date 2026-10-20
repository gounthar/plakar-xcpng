package main

import (
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"

	"integration-xcpng/exporter"
)

func main() {
	if exporter.IsWatchdog(os.Args) {
		os.Exit(exporter.RunWatchdog(os.Stdin))
	}
	sdk.EntrypointExporter(os.Args, exporter.NewExporter)
}
