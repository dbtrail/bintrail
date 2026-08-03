package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func day(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

func TestObjectLockVerdict(t *testing.T) {
	tests := []struct {
		name       string
		st         objectLockState
		retain     time.Duration
		wantStatus CheckStatus
		wantDetail string // substring
	}{
		{
			name:       "unreadable config is SKIP, never a posture claim",
			st:         objectLockState{queryErr: errors.New("api error AccessDenied")},
			retain:     day(30),
			wantStatus: StatusSkip,
			wantDetail: "could not read",
		},
		{
			name:       "lock disabled warns",
			st:         objectLockState{notEnabled: true},
			retain:     day(30),
			wantStatus: StatusWarn,
			wantDetail: "does not have S3 Object Lock enabled",
		},
		{
			name:       "enabled without default rule warns (uploads set no per-object retention)",
			st:         objectLockState{},
			retain:     day(30),
			wantStatus: StatusWarn,
			wantDetail: "NO default retention rule",
		},
		{
			name:       "retention shorter than the rotation window warns",
			st:         objectLockState{mode: "COMPLIANCE", retention: day(7)},
			retain:     day(30),
			wantStatus: StatusWarn,
			wantDetail: "SHORTER than the index retention window",
		},
		{
			name:       "retention covering the window passes",
			st:         objectLockState{mode: "COMPLIANCE", retention: day(30)},
			retain:     day(30),
			wantStatus: StatusPass,
			wantDetail: "default retention COMPLIANCE 30d",
		},
		{
			name:       "no rotation configured skips the comparison but still passes on posture",
			st:         objectLockState{mode: "COMPLIANCE", retention: day(7)},
			retain:     0,
			wantStatus: StatusPass,
			wantDetail: "7d",
		},
		{
			name:       "governance pass names its bypass escape hatch",
			st:         objectLockState{mode: "GOVERNANCE", retention: day(90)},
			retain:     day(30),
			wantStatus: StatusPass,
			wantDetail: "s3:BypassGovernanceRetention",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := objectLockVerdict("my-bucket", tt.st, tt.retain)
			if got.Name != ObjectLockCheckName {
				t.Fatalf("Name = %q", got.Name)
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %q, want %q (detail: %s)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail %q does not contain %q", got.Detail, tt.wantDetail)
			}
		})
	}
}

// TestObjectLockVerdictNeverFails sweeps a grid of states and pins the
// advisory contract: no combination may return StatusFail — doctor must stay
// safe as a CI smoke test for operators who legitimately run without Object
// Lock. Unlike the per-case table, this survives future branch additions.
func TestObjectLockVerdictNeverFails(t *testing.T) {
	states := []objectLockState{
		{},
		{queryErr: errors.New("boom")},
		{notEnabled: true},
		{mode: "COMPLIANCE"},
		{mode: "GOVERNANCE", retention: day(7)},
		{mode: "COMPLIANCE", retention: day(30)},
		{retention: day(7)}, // non-AWS: retention without a mode
	}
	for _, st := range states {
		for _, retain := range []time.Duration{0, day(7), day(30)} {
			if got := objectLockVerdict("b", st, retain); got.Status == StatusFail {
				t.Fatalf("state %+v retain %v returned StatusFail — the check is advisory", st, retain)
			}
		}
	}
}

// mockLockAPI returns a fixed response or error.
type mockLockAPI struct {
	out *s3.GetObjectLockConfigurationOutput
	err error
}

func (m mockLockAPI) GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	return m.out, m.err
}

func TestFetchObjectLockState(t *testing.T) {
	ctx := context.Background()

	t.Run("NotFound API error means lock disabled, not a query failure", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{err: &smithy.GenericAPIError{Code: "ObjectLockConfigurationNotFoundError"}}, "b")
		if !st.notEnabled || st.queryErr != nil {
			t.Fatalf("got %+v, want notEnabled", st)
		}
	})

	t.Run("other errors are query failures, never a posture claim", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{err: &smithy.GenericAPIError{Code: "AccessDenied"}}, "b")
		if st.queryErr == nil || st.notEnabled {
			t.Fatalf("got %+v, want queryErr", st)
		}
	})

	t.Run("plain non-API errors (network/timeout) are query failures, never lock-disabled", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{err: errors.New("dial tcp: i/o timeout")}, "b")
		if st.queryErr == nil || st.notEnabled {
			t.Fatalf("got %+v, want queryErr", st)
		}
	})

	t.Run("enabled with a Days default rule", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{out: &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{
				ObjectLockEnabled: types.ObjectLockEnabledEnabled,
				Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{
					Mode: types.ObjectLockRetentionModeCompliance,
					Days: aws.Int32(30),
				}},
			},
		}}, "b")
		if st.retention != day(30) || st.mode != "COMPLIANCE" {
			t.Fatalf("got %+v", st)
		}
	})

	t.Run("enabled with a Years default rule", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{out: &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{
				ObjectLockEnabled: types.ObjectLockEnabledEnabled,
				Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{
					Mode:  types.ObjectLockRetentionModeGovernance,
					Years: aws.Int32(1),
				}},
			},
		}}, "b")
		if st.retention != day(365) || st.mode != "GOVERNANCE" {
			t.Fatalf("got %+v", st)
		}
	})

	t.Run("enabled without a rule", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{out: &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
		}}, "b")
		if st.notEnabled || st.queryErr != nil || st.retention != 0 || st.mode != "" {
			t.Fatalf("got %+v, want enabled with zero rule", st)
		}
	})

	t.Run("nil configuration counts as disabled", func(t *testing.T) {
		st := fetchObjectLockState(ctx, mockLockAPI{out: &s3.GetObjectLockConfigurationOutput{}}, "b")
		if !st.notEnabled {
			t.Fatalf("got %+v, want notEnabled", st)
		}
	})
}

func TestCheckArchiveObjectLockBadURL(t *testing.T) {
	got := CheckArchiveObjectLock(context.Background(), "not-an-s3-url", "", day(30))
	if got.Status != StatusWarn || !strings.Contains(got.Detail, "invalid --archive-s3") {
		t.Fatalf("got %+v", got)
	}
}
