package main

import (
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"

	"integration-xcpng/exporter"
)

func main() {
	sdk.EntrypointExporter(os.Args, exporter.NewExporter)
}
