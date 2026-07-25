package mcptools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// unreachableResolve fails the test if a handler reaches connection resolution
// — every case below must be refused before any DB work happens.
func unreachableResolve(t *testing.T) ResolveTarget {
	t.Helper()
	return func(ctx context.Context, argDSN string) (*Target, error) {
		t.Fatalf("Resolve must not be called (argDSN=%q)", argDSN)
		return nil, nil
	}
}

func callReconstruct(t *testing.T, cfg Config, args ReconstructArgs) *mcp.CallToolResult {
	t.Helper()
	res, _, err := MakeReconstructTool(cfg)(context.Background(), nil, args)
	if err != nil {
		t.Fatalf("handler returned a protocol error: %v", err)
	}
	return res
}

// assertToolError requires a tool-level error result whose text contains want.
func assertToolError(t *testing.T, res *mcp.CallToolResult, want string) {
	t.Helper()
	if res == nil || !res.IsError {
		t.Fatalf("expected a tool error, got %q", resultText(res))
	}
	if got := resultText(res); !strings.Contains(got, want) {
		t.Errorf("error text %q does not mention %q", got, want)
	}
}

// TestReconstructToolRegistration pins that reconstruct is opt-in per surface:
// a Config without it advertises exactly the four always-on tools, and setting
// it adds reconstruct annotated read-only + idempotent like its siblings.
func TestReconstructToolRegistration(t *testing.T) {
	listTools := func(t *testing.T, cfg Config) map[string]*mcp.Tool {
		t.Helper()
		ctx := context.Background()
		clientT, serverT := mcp.NewInMemoryTransports()
		server := NewServer(cfg)
		ss, err := server.Connect(ctx, serverT, nil)
		if err != nil {
			t.Fatalf("server connect: %v", err)
		}
		defer ss.Close()

		client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "2025-06-18"}, nil)
		cs, err := client.Connect(ctx, clientT, nil)
		if err != nil {
			t.Fatalf("client connect: %v", err)
		}
		defer cs.Close()

		res, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Fatalf("ListTools: %v", err)
		}
		out := map[string]*mcp.Tool{}
		for _, tool := range res.Tools {
			out[tool.Name] = tool
		}
		return out
	}

	t.Run("opt-out surface omits it", func(t *testing.T) {
		tools := listTools(t, Config{Resolve: unreachableResolve(t)})
		if _, ok := tools["reconstruct"]; ok {
			t.Error("reconstruct must not be advertised when Config.Reconstruct is false")
		}
		if len(tools) != 4 {
			t.Errorf("expected the 4 always-on tools, got %d: %v", len(tools), tools)
		}
	})

	t.Run("opt-in surface advertises it", func(t *testing.T) {
		tools := listTools(t, Config{Resolve: unreachableResolve(t), Reconstruct: true})
		tool, ok := tools["reconstruct"]
		if !ok {
			t.Fatalf("reconstruct not advertised; got %v", tools)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint {
			t.Errorf("reconstruct must be annotated read-only + idempotent, got %+v", tool.Annotations)
		}
		// The parameters must reach the client as a schema, so an agent does not
		// have to discover them by trial and error. The SDK hands the client an
		// opaque schema value, so assert over its JSON form.
		if tool.InputSchema == nil {
			t.Fatal("reconstruct has no input schema")
		}
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("marshal input schema: %v", err)
		}
		for _, want := range []string{"schema", "table", "pk", "at", "history", "allow_gaps", "baseline_dir", "baseline_s3"} {
			if !strings.Contains(string(raw), `"`+want+`"`) {
				t.Errorf("input schema is missing property %q: %s", want, raw)
			}
		}
	})
}

// TestReconstructRejectsRoutedSurfaceParams pins that a surface owning its own
// routing refuses the client-supplied DSN and baseline location BEFORE
// resolving a connection — an authenticated MCP client must not be able to
// point the console at arbitrary storage.
func TestReconstructRejectsRoutedSurfaceParams(t *testing.T) {
	cfg := Config{Resolve: unreachableResolve(t), Reconstruct: true, AllowBaselineParams: false}

	base := ReconstructArgs{Schema: "app", Table: "users", PK: "1"}

	withDSN := base
	withDSN.IndexDSN = "root@tcp(evil:3306)/x"
	assertToolError(t, callReconstruct(t, cfg, withDSN), "index_dsn is not accepted here")

	withDir := base
	withDir.BaselineDir = "/tmp/attacker"
	assertToolError(t, callReconstruct(t, cfg, withDir), "baseline_dir/baseline_s3 are not accepted here")

	withS3 := base
	withS3.BaselineS3 = "s3://attacker/prefix"
	assertToolError(t, callReconstruct(t, cfg, withS3), "baseline_dir/baseline_s3 are not accepted here")
}

// TestReconstructRequiresRowIdentity pins the three required parameters, and
// that a malformed `at` is rejected before any connection is opened.
func TestReconstructRequiresRowIdentity(t *testing.T) {
	cfg := Config{Resolve: unreachableResolve(t), Reconstruct: true, AllowBaselineParams: true}

	for _, args := range []ReconstructArgs{
		{Table: "users", PK: "1"},
		{Schema: "app", PK: "1"},
		{Schema: "app", Table: "users"},
	} {
		assertToolError(t, callReconstruct(t, cfg, args), "schema, table, and pk are all required")
	}

	assertToolError(t, callReconstruct(t, cfg,
		ReconstructArgs{Schema: "app", Table: "users", PK: "1", At: "not-a-time"}), "invalid at")
}

// TestReconstructRefusedUnderProfile pins the RBAC belt: a baseline read
// applies no redaction, so an active access-control profile refuses the tool
// even when a baseline IS configured.
func TestReconstructRefusedUnderProfile(t *testing.T) {
	cfg := Config{
		Reconstruct: true,
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{
				ProfileActive:      true,
				FindBaseline:       BaselineSource(t.TempDir()),
				BaselineConfigured: true,
			}, nil
		},
	}
	assertToolError(t, callReconstruct(t, cfg, ReconstructArgs{Schema: "app", Table: "users", PK: "1"}),
		"access-control profile is active")
}

// TestReconstructMissingBaselineIsActionable pins that both surfaces refuse
// with a remediation naming `bintrail baseline` rather than returning a
// baseline-free (and therefore silently partial) row state.
func TestReconstructMissingBaselineIsActionable(t *testing.T) {
	args := ReconstructArgs{Schema: "app", Table: "users", PK: "1"}

	t.Run("routed surface without a configured baseline", func(t *testing.T) {
		cfg := Config{
			Reconstruct: true,
			Resolve: func(ctx context.Context, _ string) (*Target, error) {
				return &Target{BaselineConfigured: false}, nil
			},
		}
		assertToolError(t, callReconstruct(t, cfg, args), "time-travel isn't available for this server")
	})

	// A surface that advertises the gate as open but supplies no lookup must
	// still refuse rather than nil-panic.
	t.Run("routed surface with a nil lookup", func(t *testing.T) {
		cfg := Config{
			Reconstruct: true,
			Resolve: func(ctx context.Context, _ string) (*Target, error) {
				return &Target{BaselineConfigured: true, FindBaseline: nil}, nil
			},
		}
		assertToolError(t, callReconstruct(t, cfg, args), "time-travel isn't available for this server")
	})

	t.Run("standalone surface with no source at all", func(t *testing.T) {
		t.Setenv("BINTRAIL_BASELINE_DIR", "")
		t.Setenv("BINTRAIL_BASELINE_S3", "")
		cfg := Config{
			Reconstruct:         true,
			AllowBaselineParams: true,
			Resolve: func(ctx context.Context, _ string) (*Target, error) {
				return &Target{}, nil
			},
		}
		assertToolError(t, callReconstruct(t, cfg, args), "`bintrail baseline`")
	})
}

// TestResolveBaselineLookupPrecedence pins the standalone source precedence:
// parameters beat the environment, and a local directory beats an S3 prefix —
// matching the console's own baselineSrc resolution.
func TestResolveBaselineLookupPrecedence(t *testing.T) {
	// FindBaselineFunc is opaque, so probe the bound source by the path the
	// lookup reports in its not-found error rather than by inspecting it.
	sourceOf := func(t *testing.T, cfg Config, args ReconstructArgs) string {
		t.Helper()
		find, err := resolveBaselineLookup(cfg, &Target{}, args)
		if err != nil {
			t.Fatalf("resolveBaselineLookup: %v", err)
		}
		_, _, _, ferr := find(context.Background(), "app", "users", time.Now())
		if ferr == nil {
			t.Fatal("expected the probe lookup to fail against a nonexistent source")
		}
		return ferr.Error()
	}

	cfg := Config{Reconstruct: true, AllowBaselineParams: true}

	t.Setenv("BINTRAIL_BASELINE_DIR", "/env/dir")
	t.Setenv("BINTRAIL_BASELINE_S3", "s3://env/prefix")

	if got := sourceOf(t, cfg, ReconstructArgs{BaselineDir: "/param/dir", BaselineS3: "s3://param/prefix"}); !strings.Contains(got, "/param/dir") {
		t.Errorf("baseline_dir must win over baseline_s3 and the env; got %q", got)
	}
	if got := sourceOf(t, cfg, ReconstructArgs{BaselineS3: "s3://param/prefix"}); !strings.Contains(got, "param") {
		t.Errorf("baseline_s3 must win over the env when baseline_dir is unset; got %q", got)
	}
	if got := sourceOf(t, cfg, ReconstructArgs{}); !strings.Contains(got, "/env/dir") {
		t.Errorf("BINTRAIL_BASELINE_DIR must be the fallback; got %q", got)
	}

	t.Setenv("BINTRAIL_BASELINE_DIR", "")
	if got := sourceOf(t, cfg, ReconstructArgs{}); !strings.Contains(got, "env") {
		t.Errorf("BINTRAIL_BASELINE_S3 must be the last fallback; got %q", got)
	}
}

// TestBuildPKFilterArity pins that a composite-key arity mismatch is caught
// with a message naming the expected columns — a silently mis-zipped filter
// would match the baseline while missing the deltas.
func TestBuildPKFilterArity(t *testing.T) {
	if _, err := buildPKFilter([]string{"tenant_id", "id"}, "42"); err == nil {
		t.Fatal("expected an arity error for 1 value against a 2-column key")
	} else if !strings.Contains(err.Error(), "tenant_id, id") {
		t.Errorf("error %q should name the primary-key columns", err)
	}

	got, err := buildPKFilter([]string{"tenant_id", "id"}, "7|42")
	if err != nil {
		t.Fatalf("buildPKFilter: %v", err)
	}
	if got["tenant_id"] != "7" || got["id"] != "42" {
		t.Errorf("pk values zipped wrong: %v", got)
	}
}

// TestReconstructWarnings pins the three non-fatal caveats, in particular that
// a missing baseline row whose first delta is not an INSERT WARNS (#782) rather
// than being reported as a clean reconstruction.
func TestReconstructWarnings(t *testing.T) {
	stale := reconstruct.StaleWarning{Message: "using an older snapshot"}

	if w := reconstructWarnings(nil, reconstruct.StaleWarning{}, map[string]any{"id": 1}, nil, nil); len(w) != 0 {
		t.Errorf("a clean reconstruction must carry no warnings, got %v", w)
	}

	w := reconstructWarnings(nil, stale, map[string]any{"id": 1}, nil, nil)
	if len(w) != 1 || !strings.HasPrefix(w[0], "stale_baseline: ") {
		t.Errorf("stale fallback must surface as stale_baseline, got %v", w)
	}

	// No baseline row + an UPDATE first: PK-change suspicion.
	updateFirst := []query.ResultRow{{EventID: 1, EventType: event.EventUpdate}}
	w = reconstructWarnings(nil, reconstruct.StaleWarning{}, nil, updateFirst, nil)
	if len(w) != 1 || !strings.HasPrefix(w[0], "pk_change_suspected: ") {
		t.Errorf("expected a pk_change_suspected warning, got %v", w)
	}

	// No baseline row + an INSERT first: the legitimate created-after-the-baseline
	// case, which must NOT warn.
	insertFirst := []query.ResultRow{{EventID: 1, EventType: event.EventInsert}}
	if w := reconstructWarnings(nil, reconstruct.StaleWarning{}, nil, insertFirst, nil); len(w) != 0 {
		t.Errorf("a row created after the baseline must not warn, got %v", w)
	}
}

// TestReconstructWarningsCaptureGap covers the half of the #765 override that
// the shared CheckCaptureGap helper cannot do: when the caller sets allow_gaps
// over a PERMANENT capture loss, the finding has to travel back in the payload.
// The CLI's operator reads the slog.Warn on stderr; an MCP client reads nothing
// but this JSON, so a silent override would render a known-incomplete fold as a
// clean one.
func TestReconstructWarningsCaptureGap(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)

	stamped := &reconstruct.CaptureGap{
		At:     since.Add(24 * time.Hour),
		Detail: "binlogs purged before stream caught up",
		Since:  since, Until: until,
	}
	w := reconstructWarnings(nil, reconstruct.StaleWarning{}, map[string]any{"id": 1}, nil, stamped)
	if len(w) != 1 || !strings.HasPrefix(w[0], "capture_gap: ") {
		t.Fatalf("an overridden capture gap must surface as one capture_gap warning, got %v", w)
	}
	if !strings.Contains(w[0], "2026-06-02T00:00:00Z") || !strings.Contains(w[0], "binlogs purged") {
		t.Errorf("the warning must carry when the loss happened and why, got: %s", w[0])
	}

	unevaluable := &reconstruct.CaptureGap{Unevaluable: true, Since: since, Until: until}
	w = reconstructWarnings(nil, reconstruct.StaleWarning{}, map[string]any{"id": 1}, nil, unevaluable)
	if len(w) != 1 || !strings.HasPrefix(w[0], "capture_gap: ") {
		t.Fatalf("an overridden unevaluable verdict must surface as a capture_gap warning, got %v", w)
	}

	// The capture-gap warning stacks with the others rather than replacing them.
	w = reconstructWarnings(nil, reconstruct.StaleWarning{Message: "using an older snapshot"},
		map[string]any{"id": 1}, nil, stamped)
	if len(w) != 2 {
		t.Errorf("expected the capture gap alongside the stale-baseline warning, got %v", w)
	}
}

// TestReconstructCaptureGapErrorNamesToolParam guards the CLI-flag leak: the
// refusal an MCP client receives must name the tool parameter it actually has
// (allow_gaps: true), never `--allow-gaps`, which an agent can only translate
// by guessing. It must still name SOME override — a refusal with no way
// forward would be a different failure, not a fix.
func TestReconstructCaptureGapErrorNamesToolParam(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	gaps := map[string]*reconstruct.CaptureGap{
		"stamped":     {At: since.Add(time.Hour), Detail: "gap", Since: since, Until: until},
		"unevaluable": {Unevaluable: true, Since: since, Until: until},
	}
	for name, gap := range gaps {
		t.Run(name, func(t *testing.T) {
			msg := reconstructCaptureGapError(gap, "app", "users").Error()
			if strings.Contains(msg, "--allow-gaps") {
				t.Errorf("the MCP refusal must not hand the client the CLI flag, got: %s", msg)
			}
			if !strings.Contains(msg, "allow_gaps: true") {
				t.Errorf("the refusal must name the tool parameter that overrides it, got: %s", msg)
			}
			if !strings.Contains(msg, "app.users") {
				t.Errorf("the refusal must name the table, got: %s", msg)
			}
		})
	}
}
