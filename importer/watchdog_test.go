package importer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"integration-xcpng/internal/xapi"
)

func TestIsWatchdog(t *testing.T) {
	if !IsWatchdog([]string{"xcpngImporter", WatchdogArg}) {
		t.Error("watchdog argument not recognised")
	}
	if IsWatchdog([]string{"xcpngImporter"}) || IsWatchdog([]string{"xcpngImporter", "-x"}) {
		t.Error("plugin invocation taken for the watchdog")
	}
}

// hangingLookupXAPI logs in, then never answers VM.get_by_uuid, as a host that stopped
// answering; it counts the session.logout calls it gets.
type hangingLookupXAPI struct {
	mu      sync.Mutex
	logouts int
}

func (f *hangingLookupXAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string `json:"method"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch req.Method {
	case "session.login_with_password":
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "OpaqueRef:session"})
	case "session.logout":
		f.mu.Lock()
		f.logouts++
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"result": ""})
	default:
		<-r.Context().Done()
	}
}

// A watchdog whose time runs out still logs out, and its error is about the
// snapshot, not about the logout.
func TestSnapshotWatchdogLogsOutPastItsTime(t *testing.T) {
	f := &hangingLookupXAPI{}
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	config := map[string]string{
		"location":     xapi.Protocol + "://" + strings.TrimPrefix(srv.URL, "https://"),
		"tls_insecure": "true",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := destroyLeftovers(ctx, config, []string{"8d1c0c9e-0000-0000-0000-000000000001"})
	if err == nil {
		t.Fatal("no error for a snapshot left untried")
	}
	if strings.Contains(err.Error(), "session.logout") {
		t.Errorf("error names the logout: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logouts != 1 {
		t.Errorf("%d session.logout calls, want 1", f.logouts)
	}
}
