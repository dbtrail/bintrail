package cli

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestPrintVerifyReport_ExitCodes locks the exit contract, and locks it
// IDENTICALLY for both output formats (#954): --format json is a rendering
// choice, never a semantics change. A JSON consumer that trusted the exit code
// less (or more) than a text one would silently re-open the false-assurance
// hole this command exists to close.
func TestPrintVerifyReport_ExitCodes(t *testing.T) {
	r := func(status verify.Status) verify.TableResult {
		return verify.TableResult{Schema: "db", Table: "t", Status: status}
	}
	cases := []struct {
		name    string
		results []verify.TableResult
		wantErr bool // non-nil error == non-zero exit
	}{
		{"all match", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusMatch)}, false},
		{"match + inconclusive (partial success)", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusInconclusive)}, false},
		{"one mismatch fails", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusMismatch)}, true},
		{"one error fails", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusError)}, true},
		{"all inconclusive fails (nothing proven)", []verify.TableResult{r(verify.StatusInconclusive), r(verify.StatusInconclusive)}, true},
		{"unknown status counts as error", []verify.TableResult{r(verify.StatusMatch), r(verify.Status("bogus"))}, true},
		{"empty/zero status counts as error", []verify.TableResult{r(verify.StatusMatch), r(verify.Status(""))}, true},
	}
	for _, format := range []string{"text", "json"} {
		for _, tc := range cases {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				setVerifyFormat(t, format)
				cmd := &cobra.Command{}
				cmd.SetOut(&bytes.Buffer{})
				var err error
				captureVerifyStdout(t, func() {
					err = emitVerifyReport(cmd, verify.NewReport(verify.ModeBaselinePair, tc.results))
				})
				if tc.wantErr && err == nil {
					t.Errorf("want non-zero exit (error), got nil")
				}
				if !tc.wantErr && err != nil {
					t.Errorf("want exit 0 (nil), got %v", err)
				}
			})
		}
	}
}
