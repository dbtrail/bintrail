package cli

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/verify"
)

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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.SetOut(&bytes.Buffer{})
			err := printVerifyReport(cmd, tc.results)
			if tc.wantErr && err == nil {
				t.Errorf("want non-zero exit (error), got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want exit 0 (nil), got %v", err)
			}
		})
	}
}
