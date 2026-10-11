package xapi

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path"
)

// maxUEFICertificates bounds the decoded uefi_certificates field. The four files XCP-ng
// ships total about 34 KiB.
const maxUEFICertificates = 4 << 20

// ParseUEFICertificates decodes a host or pool uefi_certificates field, a base64 tar of
// the .auth files, into file name -> contents. Each host builds its tar in its own
// directory order, so two hosts with the same certificates can hold different strings.
func ParseUEFICertificates(field string) (map[string]string, error) {
	out := map[string]string{}
	if field == "" {
		return out, nil
	}
	if base64.StdEncoding.DecodedLen(len(field)) > maxUEFICertificates {
		return nil, fmt.Errorf("larger than %d bytes", maxUEFICertificates)
	}
	data, err := base64.StdEncoding.DecodeString(field)
	if err != nil {
		return nil, err
	}

	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		// The archive is in memory and bounded above, so its members are too.
		body, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		out[path.Base(hdr.Name)] = string(body)
	}
}
