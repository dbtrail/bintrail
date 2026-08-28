package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// TestSQLPanelDecimalNote is the panel's half of #1486's honesty.
//
// The panel EXECUTES the generated views and discards their text, so every
// explanation the downloadable file carries about uncast decimal columns is
// invisible to a panel user. Without a warning next to the rows, they run
// sum() on a money column, get the raw "No function matches ... 'sum(VARCHAR)'"
// and have nowhere to read why.
func TestSQLPanelDecimalNote(t *testing.T) {
	typed := views.BaselineTable{Schema: "shop", Table: "orders", SchemaKnown: true}
	untyped := views.BaselineTable{Schema: "shop", Table: "legacy"}

	if got := sqlPanelDecimalNote(views.Input{Baselines: []views.BaselineTable{typed}}); got != "" {
		t.Errorf("a fully typed layout must produce no note, got %q", got)
	}
	if got := sqlPanelDecimalNote(views.Input{}); got != "" {
		t.Errorf("a layout with no baselines must produce no note, got %q", got)
	}

	got := sqlPanelDecimalNote(views.Input{Baselines: []views.BaselineTable{typed, untyped}})
	if got == "" {
		t.Fatal("a layout with an untyped table must say so: the panel discards the generated " +
			"file's own explanation, so this is the only place a panel user can read it")
	}
	for _, want := range []string{"1 of 2", "CAST"} {
		if !strings.Contains(got, want) {
			t.Errorf("note %q is missing %q", got, want)
		}
	}

	// The note names two causes that are nobody's fault (a PostgreSQL source, a
	// pre-feature baseline) and one that is (a footer that would not open). Only
	// the SchemaKnown bit is available here, so it cannot tell which applies, and
	// a whole-batch failure such as an S3 403 clears that bit for every table at
	// once. Naming only the harmless two would tell an operator with real broken
	// credentials that nothing is wrong, so the note must keep pointing at the
	// log. This is the same rule decimalComments follows in the other direction.
	if !strings.Contains(got, "log") {
		t.Errorf("note %q never mentions the log, so a whole-batch failure (an S3 403, no "+
			"httpfs) reads as the benign PostgreSQL case and the operator is told nothing "+
			"is wrong", got)
	}

	// Reads on a single-file layout too: "1 of 1 baseline files carry" is the
	// shape this catches, and it is the one a small deployment always sees.
	one := sqlPanelDecimalNote(views.Input{Baselines: []views.BaselineTable{untyped}})
	if !strings.Contains(one, "1 of 1 baseline file carries") {
		t.Errorf("single-file note reads wrong: %q", one)
	}
}

// TestSQLPanel_warnsWhenCastsAreMissing is the WIRING half. The note above is a
// pure function; what matters is that a panel response actually carries it.
//
// The fixture is a baseline with no embedded CREATE TABLE, which is the shape
// of a pre-feature snapshot and of every PostgreSQL-source one. Its decimal
// columns cannot be cast, so a panel user's sum() over them fails, and this
// warning is the only place they can learn why.
func TestSQLPanel_warnsWhenCastsAreMissing(t *testing.T) {
	root := t.TempDir()
	writeUntypedBaseline(t, root)

	srv := newSQLPanelServer(t, root, true)
	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var res struct {
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range res.Warnings {
		// Pinned on the ACTION, not on the sentence: a user who reads this note
		// needs to come away knowing to cast. Wording that drops "explicit CAST"
		// has stopped doing the job even if it still describes the situation.
		if strings.Contains(w, "explicit CAST") {
			found = true
		}
	}
	if !found {
		t.Errorf("the panel response carries no note about the missing casts; a user hitting "+
			"sum(VARCHAR) here has nowhere to read why. warnings = %v", res.Warnings)
	}
}

// writeUntypedBaseline writes a real, DuckDB-readable baseline Parquet with NO
// CREATE TABLE in its footer.
func writeUntypedBaseline(t *testing.T, root string) {
	t.Helper()
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `total` decimal(10,2) DEFAULT NULL\n);\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	dir := filepath.Join(root, "2026-06-10T12-00-00Z", "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// No Metadata: the footer carries no schema, so nothing can be cast.
	w, err := baseline.NewWriter(filepath.Join(dir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "10.50"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestResolveBaselineDecimals_memoizesPerSnapshot proves the cache is real, by
// making the second call impossible to serve from disk.
//
// The property is not cosmetic. Server.bucketRegions states the rule this path
// is under: buildViewsInput runs on EVERY SQL panel query, so a network round
// trip does not belong there in the steady state. A footer read per table is N
// of them, unbounded in the table count, inside the panel's setup deadline and
// behind its single-flight latch. A regression here is invisible locally and
// expensive against S3, which is exactly the kind of thing that needs a test
// rather than a comment.
func TestResolveBaselineDecimals_memoizesPerSnapshot(t *testing.T) {
	root := t.TempDir()
	snap := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(root, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `total` decimal(10,2) DEFAULT NULL\n);\n"
	writeRealBaselineFixture(t, root, "2026-06-10T12-00-00Z", "shop", "orders", createSQL)

	s := &Server{}
	newInput := func() views.Input {
		return views.Input{
			BaselineSource:   root,
			BaselineSnapshot: snap,
			Baselines:        []views.BaselineTable{{Schema: "shop", Table: "orders", Path: path}},
		}
	}

	first := newInput()
	s.resolveBaselineDecimals(context.Background(), &first)
	if !first.Baselines[0].SchemaKnown || len(first.Baselines[0].Decimals) != 1 {
		t.Fatalf("first resolution did not read the schema: %+v", first.Baselines[0])
	}

	// Remove the file. Anything that reads disk now comes back empty.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	second := newInput()
	s.resolveBaselineDecimals(context.Background(), &second)
	if !second.Baselines[0].SchemaKnown || len(second.Baselines[0].Decimals) != 1 {
		t.Errorf("the second resolution went back to disk instead of the cache: %+v",
			second.Baselines[0])
	}

	// A DIFFERENT snapshot is a different key and must not be served the old
	// answer: taking a new baseline has to be picked up.
	third := newInput()
	third.BaselineSnapshot = snap.Add(time.Hour)
	s.resolveBaselineDecimals(context.Background(), &third)
	if third.Baselines[0].SchemaKnown {
		t.Error("a new snapshot was served the previous snapshot's cached column types")
	}
}
