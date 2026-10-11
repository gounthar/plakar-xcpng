package xapi

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"maps"
	"testing"
)

func certTar(t *testing.T, files ...string) string {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i := 0; i < len(files); i += 2 {
		hdr := &tar.Header{Name: "/usr/share/varstored/" + files[i], Mode: 0o644, Size: int64(len(files[i+1])), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(files[i+1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestParseUEFICertificatesIgnoresOrder(t *testing.T) {
	a, err := ParseUEFICertificates(certTar(t, "PK.auth", "pk", "KEK.auth", "kek", "db.auth", "db"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseUEFICertificates(certTar(t, "db.auth", "db", "PK.auth", "pk", "KEK.auth", "kek"))
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(a, b) {
		t.Fatalf("same files in another order differ: %v vs %v", a, b)
	}
	if a["PK.auth"] != "pk" || len(a) != 3 {
		t.Fatalf("unexpected contents %v", a)
	}

	c, err := ParseUEFICertificates(certTar(t, "PK.auth", "pk", "KEK.auth", "other", "db.auth", "db"))
	if err != nil {
		t.Fatal(err)
	}
	if maps.Equal(a, c) {
		t.Fatal("different KEK compared equal")
	}
}

func TestParseUEFICertificatesEmpty(t *testing.T) {
	m, err := ParseUEFICertificates("")
	if err != nil || len(m) != 0 {
		t.Fatalf("got %v, %v", m, err)
	}
	if _, err := ParseUEFICertificates("not base64!"); err == nil {
		t.Fatal("invalid base64 accepted")
	}
}
