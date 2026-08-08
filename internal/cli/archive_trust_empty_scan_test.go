package cli

import (
	"testing"
	"time"
)

// The flag value must NAME the vouched backend(s) — a bare boolean was the
// #1280-review trap (one vouch disarming the gate for both backends).
func TestParseTrustEmptyScan(t *testing.T) {
	cases := []struct {
		in        string
		local, s3 bool
		wantErr   bool
	}{
		{in: ""},
		{in: "local", local: true},
		{in: "s3", s3: true},
		{in: "local,s3", local: true, s3: true},
		{in: " s3 , local ", local: true, s3: true},
		{in: "true", wantErr: true},
		{in: "both", wantErr: true},
		{in: "local,", wantErr: true},
	}
	for _, c := range cases {
		local, s3, err := parseTrustEmptyScan(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("%q: err=%v, wantErr=%v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && (local != c.local || s3 != c.s3) {
			t.Errorf("%q: got local=%v s3=%v, want %v/%v", c.in, local, s3, c.local, c.s3)
		}
	}
}

// TestReconcileDiffOptionsWiring pins the flag→field wiring: the swap
// mutation (local vouch driving the S3 field) compiles and no engine test can
// see it — this is the one place that proves the parsed value lands on the
// right DiffOptions field. Also pins the inert-vouch rejection.
func TestReconcileDiffOptionsWiring(t *testing.T) {
	set := func(dir, s3, trust string) {
		arcDir, arcS3, arcTrustEmptyScan = dir, s3, trust
	}
	defer set("", "", "")
	now := time.Now().UTC()

	set("/tmp/a", "", "local")
	opts, err := reconcileDiffOptions(now)
	if err != nil || !opts.TrustEmptyLocal || opts.TrustEmptyS3 {
		t.Errorf("local vouch: want TrustEmptyLocal only, got %+v err=%v", opts, err)
	}

	set("", "s3://b/p", "s3")
	opts, err = reconcileDiffOptions(now)
	if err != nil || opts.TrustEmptyLocal || !opts.TrustEmptyS3 {
		t.Errorf("s3 vouch: want TrustEmptyS3 only, got %+v err=%v", opts, err)
	}

	// Inert vouches (naming a backend this invocation does not scan) are
	// rejected loudly, before any scan runs.
	set("/tmp/a", "", "s3")
	if _, err = reconcileDiffOptions(now); err == nil {
		t.Error("s3 vouch without --archive-s3 must be rejected")
	}
	set("", "s3://b/p", "local")
	if _, err = reconcileDiffOptions(now); err == nil {
		t.Error("local vouch without --archive-dir must be rejected")
	}
}
