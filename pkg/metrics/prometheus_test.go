package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWritePrometheus_FormatAndValues(t *testing.T) {
	Writes.Store(42)
	WriteErrors.Store(3)
	SetMemTableGauges(func() int64 { return 128 }, 1024)

	var b strings.Builder
	if err := WritePrometheus(&b); err != nil {
		t.Fatalf("WritePrometheus error: %v", err)
	}
	out := b.String()

	wants := []string{
		"# TYPE bandb_writes_total counter",
		"bandb_writes_total 42",
		"bandb_write_errors_total 3",
		"# TYPE bandb_memtable_inflight_bytes gauge",
		"bandb_memtable_inflight_bytes 128",
		"bandb_memtable_budget_bytes 1024",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("prometheus output missing %q\n---\n%s", w, out)
		}
	}
}

func TestHandler_ServesMetrics(t *testing.T) {
	Reads.Store(7)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain...", ct)
	}
	if !strings.Contains(rec.Body.String(), "bandb_reads_total 7") {
		t.Errorf("body missing bandb_reads_total 7:\n%s", rec.Body.String())
	}
}

func TestStartPrometheusServer_EmptyAddrIsNoop(t *testing.T) {
	if err := StartPrometheusServer(""); err != nil {
		t.Fatalf("empty addr should be no-op, got error: %v", err)
	}
}
