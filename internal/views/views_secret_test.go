package views

import (
	"strings"
	"testing"
)

// TestGenerate_secretIsSessionScoped (#1456) pins two things about the S3
// preamble. The secret stays a TEMPORARY one: a PERSISTENT credential_chain
// secret makes DuckDB resolve the chain at creation and write the resulting
// keys to disk, which would turn a file that promises "no credentials" into one
// that plants them. And because a temporary secret dies with the session while
// the views outlive it in a database file, the file must say so where the
// operator reads it, next to the secret.
func TestGenerate_secretIsSessionScoped(t *testing.T) {
	out := Generate(goldenInput())

	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, "PERSISTENT") {
			t.Fatalf("an executable line makes the secret persistent: %s", line)
		}
	}
	if !strings.Contains(out, "CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain") {
		t.Fatal("temporary credential_chain secret missing from the preamble")
	}
	for _, want := range []string{
		"lives only in this DuckDB session",
		".read views.sql",
		"PERSISTENT",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("preamble does not explain the session scope (missing %q):\n%s", want, out)
		}
	}
}

// A local-only layout has no secret, so it must not carry the session-scope
// explanation either: a warning about S3 credentials in a file that reads no S3
// is noise the operator learns to skip.
func TestGenerate_localOnlyHasNoSessionNote(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources = []string{"/data/archives/bintrail_id=11111111-2222-3333-4444-555555555555"}
	in.Baselines = nil
	in.BaselineSource = ""
	out := Generate(in)
	if strings.Contains(out, "lives only in this DuckDB session") {
		t.Errorf("session-scope note emitted for a layout with no S3:\n%s", out)
	}
}
