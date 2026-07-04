package forensics

import (
	"strings"
	"testing"
)

// fullCaps returns a fully-configured capability set the tests degrade from.
func fullCaps() Capabilities {
	return Capabilities{
		PerformanceSchema: PerfSchemaCapabilities{
			Enabled: true,
			Consumers: PerfSchemaConsumers{
				EventsStatementsHistory:     true,
				EventsStatementsHistoryLong: true,
			},
			ThreadsAccessible: true,
		},
		AuditLog: AuditLogCapabilities{
			Installed:  true,
			PluginName: "audit_log",
			Variant:    "percona",
			Config:     map[string]string{"audit_log_file": "/var/log/mysql/audit.log"},
		},
		ServerInfo: ServerInfo{Version: "8.0.36", VersionComment: "Percona Server (GPL)", Variant: "percona"},
	}
}

func recTitles(g SetupGuide) []string {
	titles := make([]string, len(g.Recommendations))
	for i, r := range g.Recommendations {
		titles[i] = r.Title
	}
	return titles
}

func findRec(t *testing.T, g SetupGuide, title string) SetupRecommendation {
	t.Helper()
	for _, r := range g.Recommendations {
		if r.Title == title {
			return r
		}
	}
	t.Fatalf("recommendation %q not found in %v", title, recTitles(g))
	return SetupRecommendation{}
}

func TestBuildSetupGuideFullyConfigured(t *testing.T) {
	g := BuildSetupGuide(fullCaps())
	if len(g.Recommendations) != 0 {
		t.Errorf("expected no recommendations, got %v", recTitles(g))
	}
	if !strings.Contains(g.Summary, "fully configured") {
		t.Errorf("Summary = %q, want the fully-configured summary", g.Summary)
	}
}

func TestBuildSetupGuidePerfSchemaDisabled(t *testing.T) {
	caps := fullCaps()
	caps.PerformanceSchema = PerfSchemaCapabilities{} // disabled
	caps.AuditLog = AuditLogCapabilities{}            // no plugin
	caps.ServerInfo.Variant = ""                      // unknown → treated as mysql

	g := BuildSetupGuide(caps)

	ps := findRec(t, g, "Enable Performance Schema")
	if ps.Category != "performance_schema" || ps.Priority != "high" {
		t.Errorf("perf-schema rec category/priority = %s/%s, want performance_schema/high", ps.Category, ps.Priority)
	}
	if len(ps.RuntimeSQL) != 0 {
		t.Errorf("enabling p_s needs a restart — no runtime SQL expected, got %v", ps.RuntimeSQL)
	}
	if !strings.Contains(ps.MycnfSnippet, "performance_schema = ON") {
		t.Errorf("my.cnf snippet = %q", ps.MycnfSnippet)
	}

	// Disabled p_s must NOT also emit the per-consumer recommendations
	// (they are meaningless until p_s itself is on).
	for _, title := range []string{"Enable statement history consumer", "Enable global statement history consumer"} {
		for _, r := range g.Recommendations {
			if r.Title == title {
				t.Errorf("unexpected consumer recommendation %q while p_s is disabled", title)
			}
		}
	}

	// Unknown variant falls back to the generic MySQL audit guidance.
	audit := findRec(t, g, "Install an audit log plugin")
	if audit.Category != "audit_plugin" {
		t.Errorf("audit rec category = %s", audit.Category)
	}

	for _, want := range []string{"is disabled", "no audit log plugin is installed", "See recommendations below."} {
		if !strings.Contains(g.Summary, want) {
			t.Errorf("Summary = %q, want it to contain %q", g.Summary, want)
		}
	}
}

func TestBuildSetupGuideConsumersOff(t *testing.T) {
	caps := fullCaps()
	caps.PerformanceSchema.Consumers = PerfSchemaConsumers{}

	g := BuildSetupGuide(caps)

	short := findRec(t, g, "Enable statement history consumer")
	if short.Priority != "medium" {
		t.Errorf("history consumer priority = %s, want medium", short.Priority)
	}
	if len(short.RuntimeSQL) != 1 || !strings.Contains(short.RuntimeSQL[0], "'events_statements_history'") {
		t.Errorf("history consumer RuntimeSQL = %v", short.RuntimeSQL)
	}

	long := findRec(t, g, "Enable global statement history consumer")
	if long.Priority != "high" {
		t.Errorf("history_long consumer priority = %s, want high", long.Priority)
	}
	if len(long.RuntimeSQL) != 1 || !strings.Contains(long.RuntimeSQL[0], "'events_statements_history_long'") {
		t.Errorf("history_long consumer RuntimeSQL = %v", long.RuntimeSQL)
	}
	if !strings.Contains(long.MycnfSnippet, "performance-schema-consumer-events-statements-history-long = ON") {
		t.Errorf("history_long my.cnf snippet = %q", long.MycnfSnippet)
	}

	// First letter is uppercased by the summary composition, so match past it.
	if !strings.Contains(g.Summary, "tatement history consumers are not fully enabled") {
		t.Errorf("Summary = %q", g.Summary)
	}
}

func TestBuildSetupGuideOnlyHistoryLongOff(t *testing.T) {
	caps := fullCaps()
	caps.PerformanceSchema.Consumers.EventsStatementsHistoryLong = false

	g := BuildSetupGuide(caps)
	if len(g.Recommendations) != 1 {
		t.Fatalf("expected exactly 1 recommendation, got %v", recTitles(g))
	}
	if g.Recommendations[0].Title != "Enable global statement history consumer" {
		t.Errorf("recommendation = %q", g.Recommendations[0].Title)
	}
}

func TestBuildSetupGuideAuditVariants(t *testing.T) {
	tests := []struct {
		variant       string
		wantTitle     string
		wantSQLSubstr string
	}{
		{"percona", "Install Percona Audit Log Filter Plugin", "INSTALL PLUGIN audit_log_filter SONAME 'audit_log_filter.so';"},
		{"mariadb", "Enable MariaDB Audit Plugin", "INSTALL SONAME 'server_audit';"},
		{"mysql", "Install an audit log plugin", "INSTALL PLUGIN audit_log SONAME 'audit_log.so';"},
	}
	for _, tt := range tests {
		t.Run(tt.variant, func(t *testing.T) {
			caps := fullCaps()
			caps.AuditLog = AuditLogCapabilities{}
			caps.ServerInfo.Variant = tt.variant

			g := BuildSetupGuide(caps)
			rec := findRec(t, g, tt.wantTitle)
			joined := strings.Join(rec.RuntimeSQL, "\n")
			if !strings.Contains(joined, tt.wantSQLSubstr) {
				t.Errorf("RuntimeSQL = %q, want it to contain %q", joined, tt.wantSQLSubstr)
			}
			if rec.Category != "audit_plugin" || rec.Priority != "medium" {
				t.Errorf("category/priority = %s/%s, want audit_plugin/medium", rec.Category, rec.Priority)
			}
		})
	}
}

// TestBuildSetupGuideRDSIAM pins the RDS branch: an installed audit plugin
// writing under /rdsdbdata/ yields the IAM-policy recommendation, and the
// summary keeps "RDS" capitalized (unlike Python's str.capitalize(), which
// would lowercase it to "Rds").
func TestBuildSetupGuideRDSIAM(t *testing.T) {
	caps := fullCaps()
	caps.AuditLog.Variant = "mariadb"
	caps.AuditLog.Config = map[string]string{
		"server_audit_file_path": "/rdsdbdata/log/audit/server_audit.log",
	}

	g := BuildSetupGuide(caps)
	rec := findRec(t, g, "Grant IAM permissions for RDS audit log access")
	for _, want := range []string{"rds:DescribeDBLogFiles", "rds:DownloadDBLogFilePortion"} {
		if !strings.Contains(rec.MycnfSnippet, want) {
			t.Errorf("IAM policy snippet missing %q: %s", want, rec.MycnfSnippet)
		}
	}
	if len(rec.RuntimeSQL) != 0 {
		t.Errorf("IAM rec must have no runtime SQL, got %v", rec.RuntimeSQL)
	}

	wantSummary := "Forensic data can be improved. RDS audit log requires IAM permissions for access. See recommendations below."
	if g.Summary != wantSummary {
		t.Errorf("Summary = %q, want %q", g.Summary, wantSummary)
	}
}

func TestIsRDSAuditPath(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]string
		want   bool
	}{
		{"mariadb rds path", map[string]string{"server_audit_file_path": "/rdsdbdata/log/audit/server_audit.log"}, true},
		{"percona rds path", map[string]string{"audit_log_file": "/rdsdbdata/log/audit.log"}, true},
		{"self-managed path", map[string]string{"server_audit_file_path": "/var/log/mysql/server_audit.log"}, false},
		{"no relevant keys", map[string]string{"audit_log_policy": "ALL"}, false},
		{"empty config", map[string]string{}, false},
		{"nil config", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRDSAuditPath(tt.config); got != tt.want {
				t.Errorf("isRDSAuditPath(%v) = %v, want %v", tt.config, got, tt.want)
			}
		})
	}
}

func TestCapitalizeFirst(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"abc def", "Abc def"},
		{"RDS stays RDS", "RDS stays RDS"},
		{"9 things", "9 things"},
		{"performance_schema is disabled", "Performance_schema is disabled"},
	}
	for _, tc := range tests {
		if got := capitalizeFirst(tc.in); got != tc.want {
			t.Errorf("capitalizeFirst(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
