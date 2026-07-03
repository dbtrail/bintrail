package forensics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	smithy "github.com/aws/smithy-go"
)

// ---------------------------------------------------------------------------
// Mock CloudWatch Logs client
// ---------------------------------------------------------------------------

// mockCWLClient implements cwlAPI with canned sequential pages and records
// every FilterLogEvents input for assertions.
type mockCWLClient struct {
	pages  []*cloudwatchlogs.FilterLogEventsOutput
	err    error
	calls  int
	inputs []cloudwatchlogs.FilterLogEventsInput
}

func (m *mockCWLClient) FilterLogEvents(_ context.Context, params *cloudwatchlogs.FilterLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	m.inputs = append(m.inputs, *params)
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	if m.calls > len(m.pages) {
		return &cloudwatchlogs.FilterLogEventsOutput{}, nil
	}
	return m.pages[m.calls-1], nil
}

// swapCWLFactory injects a mock client (or construction error) into the
// package-level factory for the duration of the test.
func swapCWLFactory(t *testing.T, client cwlAPI, err error) {
	t.Helper()
	orig := newCWLClient
	newCWLClient = func(context.Context, string) (cwlAPI, error) { return client, err }
	t.Cleanup(func() { newCWLClient = orig })
}

func cwlEvent(stream, msg string) cwltypes.FilteredLogEvent {
	return cwltypes.FilteredLogEvent{LogStreamName: strPtr(stream), Message: strPtr(msg)}
}

// auroraAuditLine renders an Aurora Advanced Auditing record (epoch
// microseconds timestamp, 10 fields).
func auroraAuditLine(ts time.Time, user string, connID int, q string) string {
	return fmt.Sprintf("%d,host,%s,10.0.0.1,%d,900,QUERY,mydb,'%s',0", ts.UnixMicro(), user, connID, q)
}

// ---------------------------------------------------------------------------
// resolveCWLogGroup
// ---------------------------------------------------------------------------

func TestResolveCWLogGroup(t *testing.T) {
	tests := []struct {
		name       string
		opts       AuditReadOptions
		wantGroup  string
		wantRegion string
		wantErr    bool
	}{
		{
			name:       "instance endpoint",
			opts:       AuditReadOptions{SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com"},
			wantGroup:  "/aws/rds/instance/mydb/audit",
			wantRegion: "us-west-2",
		},
		{
			name:       "aurora cluster endpoint",
			opts:       AuditReadOptions{SourceHost: "mycluster.cluster-abc123.eu-west-1.rds.amazonaws.com"},
			wantGroup:  "/aws/rds/cluster/mycluster/audit",
			wantRegion: "eu-west-1",
		},
		{
			name:       "instance endpoint with port",
			opts:       AuditReadOptions{SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com:3306"},
			wantGroup:  "/aws/rds/instance/mydb/audit",
			wantRegion: "us-west-2",
		},
		{
			name: "explicit group wins over derivation",
			opts: AuditReadOptions{
				SourceHost:         "mydb.abc123.us-west-2.rds.amazonaws.com",
				CloudWatchLogGroup: "/aws/rds/cluster/other/audit",
			},
			wantGroup:  "/aws/rds/cluster/other/audit",
			wantRegion: "us-west-2",
		},
		{
			name: "explicit group with non-RDS host leaves region to the SDK chain",
			opts: AuditReadOptions{
				SourceHost:         "db.internal.example.com",
				CloudWatchLogGroup: "/aws/rds/cluster/mydb/audit",
			},
			wantGroup:  "/aws/rds/cluster/mydb/audit",
			wantRegion: "",
		},
		{
			name:    "non-RDS host without explicit group",
			opts:    AuditReadOptions{SourceHost: "db.internal.example.com"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group, region, err := resolveCWLogGroup(tt.opts)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveCWLogGroup: %v", err)
			}
			if group != tt.wantGroup {
				t.Errorf("group = %q, want %q", group, tt.wantGroup)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// readAuditCloudWatch via ReadAuditLog
// ---------------------------------------------------------------------------

// TestReadAuditLog_CloudWatchRoundTrip: messages flow through the MariaDB
// parser into normalised events; the source needs no database connection.
func TestReadAuditLog_CloudWatchRoundTrip(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				cwlEvent("instance-1", auroraAuditLine(base, "admin", 42, "SELECT 1")),
				cwlEvent("instance-1", auroraAuditLine(base.Add(time.Second), "app", 43, "INSERT INTO t VALUES(1)")),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Source != AuditSourceCloudWatch {
		t.Errorf("Source = %q, want cloudwatch", res.Source)
	}
	if res.FilePath != "/aws/rds/instance/mydb/audit" {
		t.Errorf("FilePath = %q, want the derived log group", res.FilePath)
	}
	if res.FormatDetected != AuditFormatMariaDB || res.Variant != AuditVariantMariaDB {
		t.Errorf("format/variant = %q/%q, want mariadb/mariadb", res.FormatDetected, res.Variant)
	}
	if res.TotalScanned != 2 || res.SkippedLines != 0 {
		t.Errorf("TotalScanned/SkippedLines = %d/%d, want 2/0", res.TotalScanned, res.SkippedLines)
	}
	if res.FilesRead != 1 {
		t.Errorf("FilesRead = %d, want 1 (one stream)", res.FilesRead)
	}
	if len(res.Events) != 2 {
		t.Fatalf("got %d events, want 2", len(res.Events))
	}
	ev := res.Events[0]
	if ev.User != "admin" || ev.ConnectionID != 42 || ev.SQLText != "SELECT 1" || ev.EventType != "QUERY" || ev.DB != "mydb" {
		t.Errorf("event[0] = %+v, want the mapped admin record", ev)
	}
	// Aurora epoch-micros timestamps must come out normalised to RFC 3339.
	if ts, terr := time.Parse(time.RFC3339Nano, ev.Timestamp); terr != nil || !ts.Equal(base) {
		t.Errorf("Timestamp = %q, want RFC3339 of %v", ev.Timestamp, base)
	}
}

// TestReadAuditLog_CloudWatchMultiStreamSorted: streams (one per Aurora
// instance) interleave with no cross-stream ordering — the result must be
// sorted by the record-embedded timestamps.
func TestReadAuditLog_CloudWatchMultiStreamSorted(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				cwlEvent("instance-2", auroraAuditLine(base.Add(30*time.Second), "u", 1, "B1")),
				cwlEvent("instance-1", auroraAuditLine(base, "u", 2, "A1")),
				cwlEvent("instance-2", auroraAuditLine(base.Add(90*time.Second), "u", 3, "B2")),
				cwlEvent("instance-1", auroraAuditLine(base.Add(60*time.Second), "u", 4, "A2")),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mycluster.cluster-abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.FilesRead != 2 {
		t.Errorf("FilesRead = %d, want 2 streams", res.FilesRead)
	}
	if len(res.Events) != 4 {
		t.Fatalf("got %d events, want 4", len(res.Events))
	}
	got := []string{res.Events[0].SQLText, res.Events[1].SQLText, res.Events[2].SQLText, res.Events[3].SQLText}
	want := []string{"A1", "B1", "A2", "B2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// TestReadAuditLog_CloudWatchWindowPadding: the CloudWatch StartTime and
// EndTime are the Since/Until window padded by ±5 minutes (delivery
// timestamps lag record timestamps), while the exact window is enforced on
// the record-embedded timestamp — an event delivered inside the padded
// window but stamped outside [since, until) must be excluded.
func TestReadAuditLog_CloudWatchWindowPadding(t *testing.T) {
	since := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	until := time.Date(2026, 4, 13, 12, 10, 0, 0, time.UTC)

	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				// Record stamped 2 min before since: inside the padded
				// pre-filter, outside the exact window.
				cwlEvent("s1", auroraAuditLine(since.Add(-2*time.Minute), "u", 1, "TOO_EARLY")),
				cwlEvent("s1", auroraAuditLine(since.Add(time.Minute), "u", 2, "IN_WINDOW")),
				// Record stamped exactly at until (exclusive): out.
				cwlEvent("s1", auroraAuditLine(until, "u", 3, "AT_UNTIL")),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
		Since:      since,
		Until:      until,
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}

	if len(mock.inputs) != 1 {
		t.Fatalf("expected 1 FilterLogEvents call, got %d", len(mock.inputs))
	}
	in := mock.inputs[0]
	if in.StartTime == nil || *in.StartTime != since.Add(-5*time.Minute).UnixMilli() {
		t.Errorf("StartTime = %v, want since-5min millis", in.StartTime)
	}
	if in.EndTime == nil || *in.EndTime != until.Add(5*time.Minute).UnixMilli() {
		t.Errorf("EndTime = %v, want until+5min millis", in.EndTime)
	}

	if len(res.Events) != 1 || res.Events[0].SQLText != "IN_WINDOW" {
		t.Errorf("Events = %+v, want only IN_WINDOW (record-timestamp filtering)", res.Events)
	}
	if res.TotalScanned != 3 {
		t.Errorf("TotalScanned = %d, want 3", res.TotalScanned)
	}
}

// TestReadAuditLog_CloudWatchUnboundedWindowOmitsTimes: with no Since/Until
// the FilterLogEvents call carries no time bounds.
func TestReadAuditLog_CloudWatchUnboundedWindowOmitsTimes(t *testing.T) {
	mock := &mockCWLClient{pages: []*cloudwatchlogs.FilterLogEventsOutput{{}}}
	swapCWLFactory(t, mock, nil)

	if _, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	}); err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	in := mock.inputs[0]
	if in.StartTime != nil || in.EndTime != nil {
		t.Errorf("StartTime/EndTime = %v/%v, want nil/nil", in.StartTime, in.EndTime)
	}
}

// TestReadAuditLog_CloudWatchPagination follows NextToken to exhaustion.
func TestReadAuditLog_CloudWatchPagination(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	token := "next-page"
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{
			{
				Events:    []cwltypes.FilteredLogEvent{cwlEvent("s1", auroraAuditLine(base, "u", 1, "P1"))},
				NextToken: &token,
			},
			{
				Events: []cwltypes.FilteredLogEvent{cwlEvent("s1", auroraAuditLine(base.Add(time.Second), "u", 2, "P2"))},
			},
		},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if mock.calls != 2 {
		t.Errorf("FilterLogEvents calls = %d, want 2", mock.calls)
	}
	if in := mock.inputs[1]; in.NextToken == nil || *in.NextToken != token {
		t.Errorf("second call NextToken = %v, want %q", in.NextToken, token)
	}
	if len(res.Events) != 2 {
		t.Errorf("got %d events, want 2 across pages", len(res.Events))
	}
}

// TestReadAuditLog_CloudWatchOffsetLimit pages the sorted result.
func TestReadAuditLog_CloudWatchOffsetLimit(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	// Delivered out of order; sorted order is E0..E3.
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				cwlEvent("s1", auroraAuditLine(base.Add(3*time.Second), "u", 1, "E3")),
				cwlEvent("s1", auroraAuditLine(base, "u", 2, "E0")),
				cwlEvent("s1", auroraAuditLine(base.Add(time.Second), "u", 3, "E1")),
				cwlEvent("s1", auroraAuditLine(base.Add(2*time.Second), "u", 4, "E2")),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
		Offset:     1,
		Limit:      2,
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if len(res.Events) != 2 || res.Events[0].SQLText != "E1" || res.Events[1].SQLText != "E2" {
		t.Errorf("Events = %+v, want [E1 E2] (offset/limit after sorting)", res.Events)
	}
}

// TestReadAuditLog_CloudWatchSkipsJunkAndSplitsMultiLine: unparseable
// messages count as skipped; batched multi-line messages split into
// individual records.
func TestReadAuditLog_CloudWatchSkipsJunkAndSplitsMultiLine(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	multi := auroraAuditLine(base, "u", 1, "L1") + "\n" + auroraAuditLine(base.Add(time.Second), "u", 2, "L2")
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				cwlEvent("s1", "this is not an audit record"),
				cwlEvent("s1", multi),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.SkippedLines != 1 {
		t.Errorf("SkippedLines = %d, want 1", res.SkippedLines)
	}
	if len(res.Events) != 2 {
		t.Errorf("got %d events, want 2 from the multi-line message", len(res.Events))
	}
}

// TestReadAuditLog_CloudWatchLocalTimeNote: RDS-fork records carry
// server-local timestamps — the shared MariaDB caveat must surface once.
func TestReadAuditLog_CloudWatchLocalTimeNote(t *testing.T) {
	mock := &mockCWLClient{
		pages: []*cloudwatchlogs.FilterLogEventsOutput{{
			Events: []cwltypes.FilteredLogEvent{
				cwlEvent("s1", "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,"),
				cwlEvent("s1", "20260413 12:00:01,host,admin,10.0.0.1,42,101,QUERY,mydb,'SELECT 2',0,,"),
			},
		}},
	}
	swapCWLFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	count := 0
	for _, w := range res.Warnings {
		if w == mariadbLocalTimeNote {
			count++
		}
	}
	if count != 1 {
		t.Errorf("local-time note appears %d times in %v, want exactly once", count, res.Warnings)
	}
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

func TestReadAuditLog_CloudWatchLogGroupNotFound(t *testing.T) {
	mock := &mockCWLClient{
		err: &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: "log group does not exist"},
	}
	swapCWLFactory(t, mock, nil)

	_, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if !errors.Is(err, ErrAuditLogGroupNotFound) {
		t.Fatalf("err = %v, want ErrAuditLogGroupNotFound", err)
	}
	// Remediation must mention the Aurora cluster-level group shape.
	if !strings.Contains(err.Error(), "/aws/rds/cluster/") {
		t.Errorf("error should mention the cluster-level group: %v", err)
	}
}

func TestClassifyCWLError_AccessDenied(t *testing.T) {
	err := classifyCWLError(&smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"}, "/aws/rds/instance/mydb/audit")
	if !errors.Is(err, ErrAuditAccessDenied) {
		t.Fatalf("err = %v, want ErrAuditAccessDenied", err)
	}
	if !strings.Contains(err.Error(), "logs:FilterLogEvents") {
		t.Errorf("message should mention the missing IAM action: %v", err)
	}
}

func TestClassifyCWLError_Throttling(t *testing.T) {
	err := classifyCWLError(&smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}, "g")
	if !errors.Is(err, ErrAuditThrottled) {
		t.Fatalf("err = %v, want ErrAuditThrottled", err)
	}
}

func TestClassifyCWLError_NoCredentials(t *testing.T) {
	err := classifyCWLError(fmt.Errorf("failed to refresh cached credentials, no EC2 IMDS role found"), "g")
	if !errors.Is(err, ErrAuditNoAWSCredentials) {
		t.Fatalf("err = %v, want ErrAuditNoAWSCredentials", err)
	}
}

func TestReadAuditLog_CloudWatchRequiresHostOrGroup(t *testing.T) {
	swapCWLFactory(t, nil, nil) // must not be reached
	_, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceCloudWatch,
		SourceHost: "db.internal.example.com",
	})
	if err == nil || !strings.Contains(err.Error(), "CloudWatchLogGroup") {
		t.Fatalf("err = %v, want an actionable log-group requirement error", err)
	}
}
