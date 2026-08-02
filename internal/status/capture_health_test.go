package status

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// ─── Capture health verdict (#1034) ───────────────────────────────────────────
//
// The continuity verdict's sibling for IN-STREAM discards: events the daemon
// read and chose to drop while the checkpoint stayed fresh and continuity
// honestly said "no gaps". Rendered from stream_state.capture_skips through
// the same WriteStatus/WriteStatusJSON paths production uses.

func captureStream(captureSkips any) *StreamStateInfo {
	s := &StreamStateInfo{
		Mode:              "gtid",
		BinlogFile:        "binlog.000042",
		BinlogPosition:    99012,
		EventsIndexed:     986655,
		LastCheckpoint:    time.Date(2026, 7, 17, 12, 30, 0, 0, time.UTC),
		ServerID:          100,
		GapColumnsPresent: true,
	}
	if raw, ok := captureSkips.(string); ok {
		s.CaptureSkips = sql.NullString{String: raw, Valid: true}
	}
	return s
}

const degradedSkips = `{"column_count_mismatch":{"count":41203,"last_at":"2026-07-17T12:24:12Z"}}`

// attributedSkips carries the #999 last-seen attribution the capture daemon
// stamps for statement-format DML drops.
const attributedSkips = `{"statement_format_dml":{"count":7,"last_at":"2026-07-18T01:00:00Z",` +
	`"last_file":"binlog.000042","last_pos":99012,"last_statement_type":"UPDATE","last_connection_id":55}}`

func TestWriteStatus_captureHealthDegraded(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(degradedSkips))
	out := buf.String()
	for _, want := range []string{
		"Capture health:  ⚠ DEGRADED — 41,203 events skipped (column_count_mismatch), last 2026-07-17 12:24:12",
		"NOT indexed",
		"bintrail snapshot",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("degraded status missing %q:\n%s", want, out)
		}
	}
}

func TestWriteStatus_captureHealthMultipleReasons(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(
		`{"column_count_mismatch":{"count":1200,"last_at":"2026-07-17T12:24:12Z"},`+
			`"statement_format_dml":{"count":7,"last_at":"2026-07-18T01:00:00Z"}}`))
	out := buf.String()
	// Reasons sorted by count descending; the overall "last" is the max last_at.
	if !strings.Contains(out, "1,207 events skipped (column_count_mismatch: 1,200, statement_format_dml: 7), last 2026-07-18 01:00:00") {
		t.Errorf("multi-reason summary wrong:\n%s", out)
	}
}

// #999: the DEGRADED block renders the last attributed drop; a pre-#999 ledger
// without attribution must not render an empty "Last drop" line.
func TestWriteStatus_captureHealthAttribution(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(attributedSkips))
	if !strings.Contains(buf.String(), "Last drop:       binlog.000042:99012 (UPDATE, connection id 55)") {
		t.Errorf("attributed degraded status missing the Last drop line:\n%s", buf.String())
	}

	buf.Reset()
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(degradedSkips))
	if strings.Contains(buf.String(), "Last drop") {
		t.Errorf("unattributed ledger must omit the Last drop line:\n%s", buf.String())
	}
}

// Partial-attribution rendering: file-only drops the keyword segment, an empty
// file renders "?" (a drop before the first rotate — reachable in production),
// full attribution renders both halves.
func TestLastCaptureSkipAttribution_partial(t *testing.T) {
	at := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		st   CaptureSkipStat
		want string
	}{
		"full":         {CaptureSkipStat{LastAt: at, LastFile: "binlog.000042", LastPos: 99012, LastStatementType: "UPDATE", LastConnectionID: 55}, "binlog.000042:99012 (UPDATE, connection id 55)"},
		"file-only":    {CaptureSkipStat{LastAt: at, LastFile: "binlog.000042", LastPos: 99012}, "binlog.000042:99012"},
		"keyword-only": {CaptureSkipStat{LastAt: at, LastPos: 4242, LastStatementType: "UPDATE", LastConnectionID: 77}, "?:4242 (UPDATE, connection id 77)"},
	} {
		if got := lastCaptureSkipAttribution(map[string]CaptureSkipStat{"statement_format_dml": tc.st}); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}
	if got := lastCaptureSkipAttribution(map[string]CaptureSkipStat{"column_count_mismatch": {LastAt: at, Count: 5}}); got != "" {
		t.Errorf("unattributed-only ledger must render no attribution, got %q", got)
	}
}

// The reason key is a persistence contract shared by two deliberately
// unlinked decls — pin the mirror so a rename in one side fails here instead
// of silently splitting the ledger.
func TestCaptureSkipReasonMirrorsParser(t *testing.T) {
	if CaptureSkipReasonStatementFormatDML != parser.SkipStatementFormatDML {
		t.Fatalf("status reason %q != parser reason %q", CaptureSkipReasonStatementFormatDML, parser.SkipStatementFormatDML)
	}
}

func TestWriteStatus_captureHealthOK(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream("{}"))
	out := buf.String()
	if !strings.Contains(out, "Capture health:  OK — no events skipped") {
		t.Errorf("evaluated-and-clean must render OK:\n%s", out)
	}
	if strings.Contains(out, "DEGRADED") {
		t.Errorf("OK output must not contain DEGRADED:\n%s", out)
	}
}

// NULL capture_skips (legacy index, or no skip-aware daemon): the verdict is
// unknown — the line must be OMITTED, never asserted OK from absent data.
func TestWriteStatus_captureHealthUnknownOmitted(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(nil))
	if strings.Contains(buf.String(), "Capture health") {
		t.Errorf("unknown capture health must omit the line:\n%s", buf.String())
	}
}

// An unparseable payload is also "unknown" — omitted, not rendered as OK.
func TestWriteStatus_captureHealthUnparseableOmitted(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream("{corrupt"))
	if strings.Contains(buf.String(), "Capture health") {
		t.Errorf("unparseable capture_skips must omit the verdict:\n%s", buf.String())
	}
}

func decodeStatusJSON(t *testing.T, stream *StreamStateInfo) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, nil, nil, nil, stream); err != nil {
		t.Fatalf("WriteStatusJSON: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func TestWriteStatusJSON_captureHealthDegraded(t *testing.T) {
	out := decodeStatusJSON(t, captureStream(degradedSkips))
	stream := out["stream"].(map[string]any)
	ch, ok := stream["capture_health"].(map[string]any)
	if !ok {
		t.Fatalf("capture_health missing: %v", stream)
	}
	if ch["status"] != "degraded" {
		t.Errorf("status = %v, want degraded", ch["status"])
	}
	if ch["total_skipped"] != float64(41203) {
		t.Errorf("total_skipped = %v, want 41203", ch["total_skipped"])
	}
	if ch["last_skip_at"] != "2026-07-17 12:24:12" {
		t.Errorf("last_skip_at = %v", ch["last_skip_at"])
	}
	skipped := ch["skipped"].(map[string]any)
	reason := skipped["column_count_mismatch"].(map[string]any)
	if reason["count"] != float64(41203) {
		t.Errorf("per-reason count = %v, want 41203", reason["count"])
	}
}

// #999: the JSON per-reason stat carries the attribution fields; a ledger
// without them omits the keys (omitempty — pre-#999 consumers see no change).
func TestWriteStatusJSON_captureHealthAttribution(t *testing.T) {
	out := decodeStatusJSON(t, captureStream(attributedSkips))
	reason := out["stream"].(map[string]any)["capture_health"].(map[string]any)["skipped"].(map[string]any)["statement_format_dml"].(map[string]any)
	for key, want := range map[string]any{
		"last_file": "binlog.000042", "last_pos": float64(99012),
		"last_statement_type": "UPDATE", "last_connection_id": float64(55),
	} {
		if reason[key] != want {
			t.Errorf("%s = %v, want %v", key, reason[key], want)
		}
	}

	out = decodeStatusJSON(t, captureStream(degradedSkips))
	reason = out["stream"].(map[string]any)["capture_health"].(map[string]any)["skipped"].(map[string]any)["column_count_mismatch"].(map[string]any)
	for _, key := range []string{"last_file", "last_pos", "last_statement_type", "last_connection_id"} {
		if _, present := reason[key]; present {
			t.Errorf("unattributed reason must omit %s: %v", key, reason)
		}
	}
}

func TestWriteStatusJSON_captureHealthOK(t *testing.T) {
	out := decodeStatusJSON(t, captureStream("{}"))
	stream := out["stream"].(map[string]any)
	ch, ok := stream["capture_health"].(map[string]any)
	if !ok {
		t.Fatalf("capture_health missing on an evaluated-and-clean stream: %v", stream)
	}
	if ch["status"] != "ok" {
		t.Errorf("status = %v, want ok", ch["status"])
	}
	if _, present := ch["skipped"]; present {
		t.Errorf("ok verdict must omit the skipped map: %v", ch)
	}
}

func TestWriteStatusJSON_captureHealthUnknownOmitted(t *testing.T) {
	out := decodeStatusJSON(t, captureStream(nil))
	stream := out["stream"].(map[string]any)
	if _, present := stream["capture_health"]; present {
		t.Errorf("unknown capture health must omit the key: %v", stream)
	}
}

func TestCommaGroup(t *testing.T) {
	for in, want := range map[int64]string{
		0: "0", 7: "7", 999: "999", 1000: "1,000", 41203: "41,203", 1234567: "1,234,567",
	} {
		if got := commaGroup(in); got != want {
			t.Errorf("commaGroup(%d) = %q, want %q", in, got, want)
		}
	}
}
