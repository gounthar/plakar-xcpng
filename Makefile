GO=go
EXT=

PLAKAR  ?= plakar
VERSION ?= v0.1.0

GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
PTAR   := xcpng_$(VERSION)_$(GOOS)_$(GOARCH).ptar

all: build

build:
	${GO} build -v -o xcpngImporter${EXT} ./plugin/importer
	${GO} build -v -o xcpngExporter${EXT} ./plugin/exporter

test:
	${GO} vet ./...
	${GO} test -race ./...

package: build
	rm -f $(PTAR)
	$(PLAKAR) pkg create ./manifest.yaml $(VERSION)

uninstall:
	-$(PLAKAR) pkg rm xcpng

install: package
	$(PLAKAR) pkg add -allow-unsigned ./$(PTAR)

reinstall: uninstall install

clean:
	rm -f xcpngImporter xcpngExporter xcpng_*.ptar

.PHONY: all build test package uninstall install reinstall clean
