package main

import (
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"

	"integration-xcpng/importer"
)

func main() {
	if importer.IsWatchdog(os.Args) {
		os.Exit(importer.RunWatchdog(os.Stdin))
	}
	sdk.EntrypointImporter(os.Args, importer.NewImporter)
}
