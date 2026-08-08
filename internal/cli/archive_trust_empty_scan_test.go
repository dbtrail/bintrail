package cli

import "testing"

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
