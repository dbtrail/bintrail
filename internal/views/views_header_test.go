package views

import (
	"strings"
	"testing"
)

const routingSentence = "listed by its S3 location"

// The S3-over-local sentence describes what registry discovery DID. Roots the
// operator named with --archive-dir/--archive-s3 are listed as named, both of
// them if both were passed, so the sentence would be false there (#1456).
func TestGenerate_routingSentenceOnlyForRegistrySources(t *testing.T) {
	in := goldenInput()
	in.ArchivesFromRegistry = true
	if out := Generate(in); !strings.Contains(out, routingSentence) {
		t.Errorf("registry-discovered sources: header lacks the routing sentence:\n%s", out)
	}
	in.ArchivesFromRegistry = false
	if out := Generate(in); strings.Contains(out, routingSentence) {
		t.Errorf("explicitly named sources: header states a routing that did not happen:\n%s", out)
	}
}

// A registry that could not be read is named as such; "none registered" would
// assert a cause the caller does not know.
func TestGenerate_discoveryErrorInHeader(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources = nil
	in.ArchiveDiscoveryError = "query archive_state: SELECT command denied"
	out := Generate(in)
	if !strings.Contains(out, "--   (could not be read from archive_state: query archive_state: SELECT command denied)") {
		t.Errorf("header does not name the read failure:\n%s", out)
	}
	if strings.Contains(out, "none registered in archive_state") {
		t.Errorf("header claims an empty registry after a failed read:\n%s", out)
	}
	// The failure is a comment, never an executable line.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "SELECT command denied") && !strings.HasPrefix(line, "--") {
			t.Errorf("error text leaked into an executable line: %s", line)
		}
	}
}
