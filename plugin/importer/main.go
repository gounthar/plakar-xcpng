package main

import (
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"

	"integration-xcpng/importer"
)

func main() {
	sdk.EntrypointImporter(os.Args, importer.NewImporter)
}
