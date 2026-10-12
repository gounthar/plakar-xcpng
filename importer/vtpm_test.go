package importer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"integration-xcpng/internal/xapi"
)

// fakeXAPI answers the JSON-RPC calls vtpms makes, from fixed tables. It models what
// xapi does on VM.snapshot: the snapshot's copy of a unique vTPM has an empty state.
type fakeXAPI struct {
	vtpms    map[string][]string // VM ref -> vTPM refs
	unique   map[string]bool     // vTPM ref -> is_unique
	contents map[string]string   // vTPM ref -> contents
}

func (f *fakeXAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	arg := func(i int) string {
		var s string
		if i < len(req.Params) {
			_ = json.Unmarshal(req.Params[i], &s)
		}
		return s
	}
	var result any
	switch req.Method {
	case "session.login_with_password":
		result = "OpaqueRef:session"
	case "VM.get_VTPMs":
		result = f.vtpms[arg(1)]
	case "VTPM.get_is_unique":
		result = f.unique[arg(1)]
	case "VTPM.get_contents":
		result = f.contents[arg(1)]
	default:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": 1, "message": "MESSAGE_METHOD_UNKNOWN", "data": []string{req.Method}},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func dialFake(t *testing.T, f *fakeXAPI) *Importer {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	c, err := xapi.Dial(context.Background(), &xapi.Config{
		Host:        strings.TrimPrefix(srv.URL, "https://"),
		TLSInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Importer{client: c}
}

func TestVTPMsReadsUniqueStateFromVM(t *testing.T) {
	p := dialFake(t, &fakeXAPI{
		vtpms:    map[string][]string{"OpaqueRef:snap": {"OpaqueRef:snap-tpm"}, "OpaqueRef:vm": {"OpaqueRef:vm-tpm"}},
		unique:   map[string]bool{"OpaqueRef:snap-tpm": true, "OpaqueRef:vm-tpm": true},
		contents: map[string]string{"OpaqueRef:snap-tpm": "", "OpaqueRef:vm-tpm": "c3RhdGU="},
	})

	got, err := p.vtpms(context.Background(), "OpaqueRef:snap", "OpaqueRef:vm")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].IsUnique || got[0].Contents != "c3RhdGU=" {
		t.Fatalf("vtpms = %+v, want one unique vTPM with the VM's state", got)
	}
}

func TestVTPMsReadsSharedStateFromSnapshot(t *testing.T) {
	p := dialFake(t, &fakeXAPI{
		vtpms:    map[string][]string{"OpaqueRef:snap": {"OpaqueRef:snap-tpm"}, "OpaqueRef:vm": {"OpaqueRef:vm-tpm"}},
		unique:   map[string]bool{"OpaqueRef:snap-tpm": false, "OpaqueRef:vm-tpm": false},
		contents: map[string]string{"OpaqueRef:snap-tpm": "c25hcA==", "OpaqueRef:vm-tpm": "bGF0ZXI="},
	})

	got, err := p.vtpms(context.Background(), "OpaqueRef:snap", "OpaqueRef:vm")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].IsUnique || got[0].Contents != "c25hcA==" {
		t.Fatalf("vtpms = %+v, want the snapshot's state", got)
	}
}

func TestVTPMsUniqueWithoutVMVTPM(t *testing.T) {
	p := dialFake(t, &fakeXAPI{
		vtpms:  map[string][]string{"OpaqueRef:snap": {"OpaqueRef:snap-tpm"}},
		unique: map[string]bool{"OpaqueRef:snap-tpm": true},
	})

	if _, err := p.vtpms(context.Background(), "OpaqueRef:snap", "OpaqueRef:vm"); err == nil {
		t.Fatal("vtpms succeeded with a unique snapshot vTPM and no vTPM on the VM")
	}
}
