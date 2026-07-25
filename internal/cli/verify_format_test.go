package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/verify"
)

// setVerifyFormat sets the package-global --format value for one test and
// restores it afterwards (these globals are shared by every test in the
// package). Never call it from a t.Parallel() test.
func setVerifyFormat(t *testing.T, format string) {
	t.Helper()
	prev := vfyFormat
	t.Cleanup(func() { vfyFormat = prev })
	vfyFormat = format
}

// captureVerifyStdout collects what fn writes to os.Stdout — cliutil.OutputJSON
// encodes there rather than to cmd.OutOrStdout(). Named distinctly from the
// integration-tagged captureStdout helper in this same package, which would
// otherwise collide under -tags integration.
func captureVerifyStdout(t *testing.T, fn func()) string {
	t.Helper()
	prev := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	func() {
		defer func() {
			os.Stdout = prev
			w.Close()
		}()
		fn()
	}()
	out := <-done
	r.Close()
	return out
}

// verifyJSON is the decode-side view of the emitted document — deliberately
// written out by hand rather than reusing verify.Report, so a field rename in
// the struct shows up here as a test failure instead of silently passing.
type verifyJSON struct {
	Mode           string `json:"mode"`
	BaselineSource string `json:"baseline_source"`
	Verdict        string `json:"verdict"`
	Message        string `json:"message"`
	Tables         []struct {
		Schema          string `json:"schema"`
		Table           string `json:"table"`
		Status          string `json:"status"`
		SourceRows      int64  `json:"source_rows"`
		ReconstructRows int64  `json:"reconstruct_rows"`
		Anchor          string `json:"anchor"`
		Reason          string `json:"reason"`
	} `json:"tables"`
	Summary struct {
		Match        int `json:"match"`
		Mismatch     int `json:"mismatch"`
		Inconclusive int `json:"inconclusive"`
		Error        int `json:"error"`
		Total        int `json:"total"`
	} `json:"summary"`
	Explain []struct {
		Schema             string `json:"schema"`
		Table              string `json:"table"`
		Unavailable        string `json:"unavailable"`
		TotalDifferingRows int    `json:"total_differing_rows"`
	} `json:"explain"`
}

func emitVerifyJSON(t *testing.T, rep *verify.Report) (verifyJSON, error) {
	t.Helper()
	setVerifyFormat(t, "json")
	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	var err error
	out := captureVerifyStdout(t, func() { err = emitVerifyReport(cmd, rep) })
	var got verifyJSON
	if decErr := json.Unmarshal([]byte(out), &got); decErr != nil {
		t.Fatalf("emitted output is not valid JSON: %v\noutput:\n%s", decErr, out)
	}
	return got, err
}

// TestVerifyJSONShape pins the per-verdict document a cron/CI consumer reads:
// which table mismatched, why, and the summary counts — the information the
// exit code alone cannot carry (#954).
func TestVerifyJSONShape(t *testing.T) {
	results := []verify.TableResult{
		{Schema: "mydb", Table: "orders", Status: verify.StatusMismatch, SourceRows: 10, ReconstructRows: 9,
			Anchor: "binlog.000007:4711", Detail: "digest differs"},
		{Schema: "mydb", Table: "customers", Status: verify.StatusMatch, SourceRows: 3, ReconstructRows: 3,
			Anchor: "binlog.000007:4711"},
		{Schema: "mydb", Table: "audit", Status: verify.StatusInconclusive, Detail: "never baselined"},
	}
	rep := verify.NewReport(verify.ModeBaselinePair, results)
	rep.BaselineSource = "/data/baselines"
	got, err := emitVerifyJSON(t, rep)
	if err == nil {
		t.Errorf("want a non-zero exit on a mismatch, got nil")
	}

	if got.Mode != verify.ModeBaselinePair {
		t.Errorf("mode = %q, want %q", got.Mode, verify.ModeBaselinePair)
	}
	if got.BaselineSource != "/data/baselines" {
		t.Errorf("baseline_source = %q", got.BaselineSource)
	}
	if got.Verdict != verify.VerdictMismatch {
		t.Errorf("verdict = %q, want %q", got.Verdict, verify.VerdictMismatch)
	}
	if got.Summary.Match != 1 || got.Summary.Mismatch != 1 || got.Summary.Inconclusive != 1 ||
		got.Summary.Error != 0 || got.Summary.Total != 3 {
		t.Errorf("summary = %+v, want 1 match / 1 mismatch / 1 inconclusive / 0 error / 3 total", got.Summary)
	}
	if len(got.Tables) != 3 {
		t.Fatalf("tables = %d, want 3", len(got.Tables))
	}
	// Sorted by schema.table, so audit, customers, orders.
	byName := map[string]int{}
	for i, tbl := range got.Tables {
		byName[tbl.Schema+"."+tbl.Table] = i
	}
	mm := got.Tables[byName["mydb.orders"]]
	if mm.Status != string(verify.StatusMismatch) || mm.Reason != "digest differs" ||
		mm.SourceRows != 10 || mm.ReconstructRows != 9 || mm.Anchor != "binlog.000007:4711" {
		t.Errorf("mismatch row = %+v", mm)
	}
	if m := got.Tables[byName["mydb.customers"]]; m.Status != string(verify.StatusMatch) || m.SourceRows != 3 {
		t.Errorf("match row = %+v", m)
	}
	if inc := got.Tables[byName["mydb.audit"]]; inc.Status != string(verify.StatusInconclusive) ||
		inc.Reason != "never baselined" {
		t.Errorf("inconclusive row = %+v", inc)
	}
}

// TestVerifyJSONAllInconclusive: nothing proven must read as a failure in JSON
// too, with a verdict a consumer can branch on rather than an empty-looking
// success.
func TestVerifyJSONAllInconclusive(t *testing.T) {
	rep := verify.NewReport(verify.ModeLive, []verify.TableResult{
		{Schema: "mydb", Table: "orders", Status: verify.StatusInconclusive, Detail: "index behind the source"},
	})
	got, err := emitVerifyJSON(t, rep)
	if err == nil {
		t.Errorf("want a non-zero exit when nothing was proven, got nil")
	}
	if got.Verdict != verify.VerdictUnproven {
		t.Errorf("verdict = %q, want %q", got.Verdict, verify.VerdictUnproven)
	}
	if got.Mode != verify.ModeLive {
		t.Errorf("mode = %q, want %q", got.Mode, verify.ModeLive)
	}
}

// TestVerifyJSONNoPredecessor: the one prose line the baseline-pair path prints
// outside the report has a JSON form, and still exits 0.
func TestVerifyJSONNoPredecessor(t *testing.T) {
	rep := verify.NewNoPredecessorReport(verify.ModeBaselinePair, "/data/baselines", "only one baseline under the source")
	got, err := emitVerifyJSON(t, rep)
	if err != nil {
		t.Errorf("want exit 0 for a single baseline, got %v", err)
	}
	if got.Verdict != verify.VerdictNoPredecessor {
		t.Errorf("verdict = %q, want %q", got.Verdict, verify.VerdictNoPredecessor)
	}
	if !strings.Contains(got.Message, "only one baseline") {
		t.Errorf("message = %q", got.Message)
	}
	if len(got.Tables) != 0 {
		t.Errorf("tables = %+v, want none", got.Tables)
	}
}

// TestVerifyJSONExplainEntry: a drill-down that could not be produced is
// reported as a field, not as prose on stdout that would corrupt the document.
func TestVerifyJSONExplainEntry(t *testing.T) {
	rep := verify.NewReport(verify.ModeBaselinePair, []verify.TableResult{
		{Schema: "mydb", Table: "orders", Status: verify.StatusMismatch},
	})
	rep.Explain = []verify.ExplainReport{{Schema: "mydb", Table: "orders", Unavailable: "baseline unreadable"}}
	got, _ := emitVerifyJSON(t, rep)
	if len(got.Explain) != 1 || got.Explain[0].Unavailable != "baseline unreadable" {
		t.Errorf("explain = %+v", got.Explain)
	}
}

// TestVerifyTextFormatUnchanged: the default rendering still emits the same
// table and summary line an existing script may be scraping.
func TestVerifyTextFormatUnchanged(t *testing.T) {
	setVerifyFormat(t, "text")
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	rep := verify.NewReport(verify.ModeBaselinePair, []verify.TableResult{
		{Schema: "mydb", Table: "orders", Status: verify.StatusMatch, SourceRows: 5, ReconstructRows: 5},
		{Schema: "mydb", Table: "audit", Status: verify.StatusInconclusive, Detail: "never baselined"},
	})
	if err := emitVerifyReport(cmd, rep); err != nil {
		t.Fatalf("emitVerifyReport: %v", err)
	}
	s := out.String()
	for _, want := range []string{"TABLE", "STATUS", "ROWS(src/recon)", "mydb.orders", "5/5",
		"never baselined", "1 match, 0 mismatch, 1 inconclusive, 0 error"} {
		if !strings.Contains(s, want) {
			t.Errorf("text output missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "{") {
		t.Errorf("text output looks like JSON:\n%s", s)
	}
}

// TestVerifyInvalidFormat: an unsupported --format is rejected up front, before
// any database connection is attempted.
func TestVerifyInvalidFormat(t *testing.T) {
	setVerifyFormat(t, "yaml")
	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	err := runVerify(cmd, nil)
	if err == nil {
		t.Fatal("want an error for --format yaml, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("want an invalid --format error, got %v", err)
	}
}

// TestVerifyFormatFlagRegistered guards the wiring the {"error":...} stderr
// convention depends on: both roots' wantsJSON shim looks the flag up by name
// on the invoked command, so a missing/renamed flag would silently drop verify
// back to plain-text errors under --format json.
func TestVerifyFormatFlagRegistered(t *testing.T) {
	f := verifyCmd.Flags().Lookup("format")
	if f == nil {
		t.Fatal("verify has no --format flag")
	}
	if f.DefValue != "text" {
		t.Errorf("--format default = %q, want text", f.DefValue)
	}
}
