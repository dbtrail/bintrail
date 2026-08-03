package shim

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeGUCsBaseline writes a minimal real baseline Parquet file with the given
// key-value metadata and returns its path — the same shape FindBaseline hands
// to snapshotSincePos.
func writeGUCsBaseline(t *testing.T, meta map[string]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "2026-01-01T00-00-00Z", "shop", "orders.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		Metadata:     meta,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return path
}

// TestSnapshotSincePosRenderGUCsWarn pins the #921 shim-side warning through
// the real metadata-read path both _snapshot folds pay (snapshotSincePos): a
// PostgreSQL baseline (LSN anchor) whose rendering-GUC stamp is absent or
// differs from the current pin logs a server-side Warn; a matching stamp or a
// MySQL baseline (no LSN) stays silent. The check must fire even though a PG
// baseline has no binlog file/pos anchor (the early-return below the read).
func TestSnapshotSincePosRenderGUCsWarn(t *testing.T) {
	cases := []struct {
		name     string
		meta     map[string]string
		wantWarn bool
	}{
		{
			name: "pg mismatched stamp warns",
			meta: map[string]string{
				baseline.MetaKeyLSN:        "42",
				baseline.MetaKeyRenderGUCs: "TimeZone=America/New_York;DateStyle=SQL;extra_float_digits=0;bytea_output=escape;IntervalStyle=sql_standard",
			},
			wantWarn: true,
		},
		{
			name:     "pg pre-pin baseline (no stamp) warns",
			meta:     map[string]string{baseline.MetaKeyLSN: "42"},
			wantWarn: true,
		},
		{
			name: "pg matching stamp is silent",
			meta: map[string]string{
				baseline.MetaKeyLSN:        "42",
				baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned,
			},
			wantWarn: false,
		},
		{
			name:     "mysql baseline (no LSN) is silent",
			meta:     map[string]string{},
			wantWarn: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeGUCsBaseline(t, tc.meta)
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))

			pos := snapshotSincePos(context.Background(), path, logger, "shop", "orders")
			if pos != nil {
				// None of the cases record a binlog file/pos anchor.
				t.Errorf("unexpected binlog anchor: %+v", pos)
			}
			gotWarn := strings.Contains(buf.String(), "rendering-GUC stamp does not match the current pin")
			if gotWarn != tc.wantWarn {
				t.Errorf("warn logged=%v, want %v; log output:\n%s", gotWarn, tc.wantWarn, buf.String())
			}
			if tc.wantWarn && !strings.Contains(buf.String(), "bintrail-pg baseline") {
				t.Errorf("warning lacks the remediation; log output:\n%s", buf.String())
			}
		})
	}
}
