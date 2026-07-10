package metrics

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
)

// metricPrefix 是所有导出指标的统一前缀。
const metricPrefix = "bandb_"

// WritePrometheus 把当前指标快照按 Prometheus 文本 exposition 格式写入 w。
// 仅依赖标准库，不引入 prometheus client。
func WritePrometheus(w io.Writer) error {
	s := Take()
	var b strings.Builder

	writeCounter(&b, "frames_dropped_malformed_total", "Frames dropped by ingest hook as malformed.", s.DroppedMalformed)
	writeCounter(&b, "frames_dropped_oversized_total", "Frames dropped by ingest hook for oversized value.", s.DroppedOversized)
	writeCounter(&b, "frames_dropped_non_monotonic_total", "Frames dropped by ingest hook for non-monotonic timestamp.", s.DroppedNonMonotonic)
	writeCounter(&b, "writes_total", "Successful PUT operations.", s.Writes)
	writeCounter(&b, "reads_total", "GET operations.", s.Reads)
	writeCounter(&b, "scans_total", "Range SCAN operations.", s.Scans)
	writeCounter(&b, "deletes_total", "Successful DELETE operations.", s.Deletes)
	writeCounter(&b, "write_errors_total", "Failed write/delete operations.", s.WriteErrors)
	writeCounter(&b, "backpressure_stalls_total", "Writes stalled by byte-credit backpressure.", s.BackpressureStalls)
	writeGauge(&b, "memtable_inflight_bytes", "Unflushed MemTable bytes (active+dirty).", s.MemTableInflightBytes)
	writeGauge(&b, "memtable_budget_bytes", "MemTable byte backpressure budget (0=disabled).", s.MemTableBudgetBytes)

	_, err := io.WriteString(w, b.String())
	return err
}

func writeCounter(b *strings.Builder, name, help string, v int64) {
	writeMetric(b, name, help, "counter", v)
}

func writeGauge(b *strings.Builder, name, help string, v int64) {
	writeMetric(b, name, help, "gauge", v)
}

func writeMetric(b *strings.Builder, name, help, typ string, v int64) {
	full := metricPrefix + name
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", full, help, full, typ, full, v)
}

// Handler 返回一个仅提供 /metrics 的 HTTP handler，输出 Prometheus 文本格式。
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = WritePrometheus(w)
	})
	return mux
}

// StartPrometheusServer 在 addr 上启动一个仅提供 /metrics 的 HTTP server。
// addr 为空表示不启用可观测端口，直接返回 nil（边缘设备默认关闭）。
// 绑定成功后在后台 Serve，返回值只反映绑定阶段的错误。
func StartPrometheusServer(addr string) error {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics listen failed: %w", err)
	}
	srv := &http.Server{Handler: Handler()}
	slog.Info("metrics endpoint started", "addr", addr, "path", "/metrics")
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics endpoint stopped", "error", err)
		}
	}()
	return nil
}
