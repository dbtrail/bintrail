package forensics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	smithy "github.com/aws/smithy-go"
)

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
func int64Ptr(n int64) *int64 { return &n }

// ---------------------------------------------------------------------------
// Host parsing
// ---------------------------------------------------------------------------

func TestParseRDSHost(t *testing.T) {
	tests := []struct {
		name       string
		hostname   string
		wantID     string
		wantRegion string
		wantOK     bool
	}{
		{
			name:       "standard RDS endpoint",
			hostname:   "abi-test.c7cq6oyes54i.us-west-2.rds.amazonaws.com",
			wantID:     "abi-test",
			wantRegion: "us-west-2",
			wantOK:     true,
		},
		{
			name:       "us-east-1 endpoint",
			hostname:   "mydb.abc123xyz.us-east-1.rds.amazonaws.com",
			wantID:     "mydb",
			wantRegion: "us-east-1",
			wantOK:     true,
		},
		{
			name:       "aurora cluster endpoint",
			hostname:   "prod-db.cluster-abc123.eu-west-1.rds.amazonaws.com",
			wantID:     "prod-db",
			wantRegion: "eu-west-1",
			wantOK:     true,
		},
		{name: "private IP", hostname: "10.0.0.5", wantOK: false},
		{name: "custom hostname", hostname: "myhost.example.com", wantOK: false},
		{name: "empty string", hostname: "", wantOK: false},
		{name: "localhost", hostname: "localhost", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, region, ok := parseRDSHost(tt.hostname)
			if ok != tt.wantOK {
				t.Fatalf("parseRDSHost(%q) ok = %v, want %v", tt.hostname, ok, tt.wantOK)
			}
			if ok {
				if id != tt.wantID {
					t.Errorf("dbIdentifier = %q, want %q", id, tt.wantID)
				}
				if region != tt.wantRegion {
					t.Errorf("region = %q, want %q", region, tt.wantRegion)
				}
			}
		})
	}
}

func TestIsRDSClusterEndpoint(t *testing.T) {
	tests := []struct {
		hostname string
		want     bool
	}{
		{"mydb.cluster-abc123.us-west-2.rds.amazonaws.com", true},
		{"mydb.cluster-ro-abc123.us-west-2.rds.amazonaws.com", true},
		{"mydb.abc123.us-west-2.rds.amazonaws.com", false},
		{"localhost", false},
	}
	for _, tt := range tests {
		if got := isRDSClusterEndpoint(tt.hostname); got != tt.want {
			t.Errorf("isRDSClusterEndpoint(%q) = %v, want %v", tt.hostname, got, tt.want)
		}
	}
}

func TestNormalizeAuditHost(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"MyDB.ABC123.us-west-2.rds.amazonaws.com", "mydb.abc123.us-west-2.rds.amazonaws.com"},
		{"mydb.abc123.us-west-2.rds.amazonaws.com:3306", "mydb.abc123.us-west-2.rds.amazonaws.com"},
		{" localhost ", "localhost"},
		{"10.0.0.5:3306", "10.0.0.5"},
	}
	for _, tt := range tests {
		if got := normalizeAuditHost(tt.in); got != tt.want {
			t.Errorf("normalizeAuditHost(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFormatFromVariant(t *testing.T) {
	tests := []struct {
		variant AuditVariant
		want    AuditFormat
	}{
		{AuditVariantMariaDB, AuditFormatMariaDB},
		{AuditVariantPercona, AuditFormatCSV},
		{AuditVariantMySQLEnterprise, AuditFormatJSON},
		{AuditVariant("bogus"), AuditFormatUnknown},
		{AuditVariant(""), AuditFormatUnknown},
	}
	for _, tt := range tests {
		if got := formatFromVariant(tt.variant); got != tt.want {
			t.Errorf("formatFromVariant(%q) = %q, want %q", tt.variant, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Mock RDS client
// ---------------------------------------------------------------------------

// mockRDSClient implements rdsLogAPI with canned, sequential responses and
// records every DownloadDBLogFilePortion input for assertions.
type mockRDSClient struct {
	describePages []*rds.DescribeDBLogFilesOutput
	describeErr   error
	describeCalls int

	downloadPages  []*rds.DownloadDBLogFilePortionOutput
	downloadErr    error
	downloadCalls  int
	downloadInputs []rds.DownloadDBLogFilePortionInput
}

func (m *mockRDSClient) DescribeDBLogFiles(_ context.Context, _ *rds.DescribeDBLogFilesInput, _ ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error) {
	m.describeCalls++
	if m.describeErr != nil {
		return nil, m.describeErr
	}
	if m.describeCalls > len(m.describePages) {
		return &rds.DescribeDBLogFilesOutput{}, nil
	}
	return m.describePages[m.describeCalls-1], nil
}

func (m *mockRDSClient) DownloadDBLogFilePortion(_ context.Context, params *rds.DownloadDBLogFilePortionInput, _ ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error) {
	m.downloadInputs = append(m.downloadInputs, *params)
	if m.downloadErr != nil {
		return nil, m.downloadErr
	}
	if m.downloadCalls >= len(m.downloadPages) {
		empty := ""
		return &rds.DownloadDBLogFilePortionOutput{LogFileData: &empty, AdditionalDataPending: boolPtr(false)}, nil
	}
	resp := m.downloadPages[m.downloadCalls]
	m.downloadCalls++
	return resp, nil
}

// swapRDSFactory injects a mock client (or construction error) into the
// package-level factory for the duration of the test.
func swapRDSFactory(t *testing.T, client rdsLogAPI, err error) {
	t.Helper()
	orig := newRDSLogClient
	newRDSLogClient = func(context.Context, string) (rdsLogAPI, error) { return client, err }
	t.Cleanup(func() { newRDSLogClient = orig })
}

// describePage builds a one-page DescribeDBLogFiles response.
func describePage(marker string, files ...rdstypes.DescribeDBLogFilesDetails) *rds.DescribeDBLogFilesOutput {
	out := &rds.DescribeDBLogFilesOutput{DescribeDBLogFiles: files}
	if marker != "" {
		out.Marker = &marker
	}
	return out
}

func logFile(name string, lastWritten int64) rdstypes.DescribeDBLogFilesDetails {
	return rdstypes.DescribeDBLogFilesDetails{LogFileName: strPtr(name), LastWritten: int64Ptr(lastWritten)}
}

// ---------------------------------------------------------------------------
// rdsLogReader
// ---------------------------------------------------------------------------

func TestRDSLogReader_SinglePage(t *testing.T) {
	data := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != data {
		t.Errorf("got %q, want %q", string(got), data)
	}
}

func TestRDSLogReader_MultiPage(t *testing.T) {
	page1 := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n20260413 12:00:"
	page2 := "01,host,admin,10.0.0.1,42,101,QUERY,mydb,'SELECT 2',0,,\n"
	marker := "page2"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &page1, AdditionalDataPending: boolPtr(true), Marker: &marker},
			{LogFileData: &page2, AdditionalDataPending: boolPtr(false)},
		},
	}

	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := page1 + page2; string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	if mock.downloadCalls != 2 {
		t.Errorf("download calls = %d, want 2", mock.downloadCalls)
	}
	// The resumed call must carry the marker from page 1.
	if in := mock.downloadInputs[1]; in.Marker == nil || *in.Marker != marker {
		t.Errorf("second call Marker = %v, want %q", in.Marker, marker)
	}
}

// semanticRDSClient emulates DownloadDBLogFilePortion's real Marker semantics
// for the full-scan regression test: a call with neither Marker nor
// NumberOfLines returns only the tail (per the AWS API, the most-recent portion
// of the file); Marker="0" returns from the start.
type semanticRDSClient struct {
	fromStart string
	tail      string
	inputs    []rds.DownloadDBLogFilePortionInput
	served    bool
}

func (m *semanticRDSClient) DescribeDBLogFiles(context.Context, *rds.DescribeDBLogFilesInput, ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error) {
	return &rds.DescribeDBLogFilesOutput{}, nil
}

func (m *semanticRDSClient) DownloadDBLogFilePortion(_ context.Context, params *rds.DownloadDBLogFilePortionInput, _ ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error) {
	m.inputs = append(m.inputs, *params)
	if m.served {
		empty := ""
		return &rds.DownloadDBLogFilePortionOutput{LogFileData: &empty, AdditionalDataPending: boolPtr(false)}, nil
	}
	m.served = true
	data := m.tail
	if params.Marker != nil && *params.Marker == "0" {
		data = m.fromStart
	}
	return &rds.DownloadDBLogFilePortionOutput{LogFileData: &data, AdditionalDataPending: boolPtr(false)}, nil
}

// TestRDSLogReader_FullScanStartsFromBeginning guards the blocker fix: a
// non-tail (full) scan MUST send Marker="0" on the first
// DownloadDBLogFilePortion call. With neither Marker nor NumberOfLines the RDS
// API returns only the most-recent ~10000 lines / 1 MB (the tail), silently
// dropping older records — precisely the who-changed full-scan path
// (auditReadOptionsFor sets TailLines:-1). The semantic mock reproduces that
// AWS behaviour, so this test fails on the pre-fix reader.
func TestRDSLogReader_FullScanStartsFromBeginning(t *testing.T) {
	const oldRec = "20260413 09:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'oldest',0,,\n"
	const newRec = "20260413 12:00:00,host,admin,10.0.0.1,42,101,QUERY,mydb,'newest',0,,\n"

	sem := &semanticRDSClient{fromStart: oldRec + newRec, tail: newRec}
	reader := newRDSLogReader(context.Background(), sem, "test-db", "audit/server_audit.log")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(sem.inputs) == 0 {
		t.Fatal("no DownloadDBLogFilePortion call recorded")
	}
	if first := sem.inputs[0]; first.Marker == nil || *first.Marker != "0" {
		t.Errorf("first full-scan call Marker = %v, want \"0\"; without it the RDS API returns only the tail and drops older records", first.Marker)
	}
	if !strings.Contains(string(got), "oldest") {
		t.Error("full scan dropped the oldest record — the reader read only the tail (Marker=\"0\" not sent on the first call)")
	}
	if !strings.Contains(string(got), "newest") {
		t.Error("full scan missing the newest record")
	}
}

// TestRDSLogReader_EmptyPageWithPendingContinues guards against silently
// truncating the scan: DownloadDBLogFilePortion may return an empty chunk with
// AdditionalDataPending=true mid-file (e.g. an actively-written log). The reader
// must follow the marker to the next page, not treat the empty chunk as EOF.
func TestRDSLogReader_EmptyPageWithPendingContinues(t *testing.T) {
	empty := ""
	realData := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	m1 := "m1"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &empty, AdditionalDataPending: boolPtr(true), Marker: &m1},
			{LogFileData: &realData, AdditionalDataPending: boolPtr(false)},
		},
	}
	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != realData {
		t.Errorf("got %q, want %q — an empty page with AdditionalDataPending=true must not end the scan", string(got), realData)
	}
	if mock.downloadCalls != 2 {
		t.Errorf("download calls = %d, want 2 (must fetch past the empty page)", mock.downloadCalls)
	}
}

// TestRDSLogReader_PendingWithoutMarkerErrors: if AWS reports more data pending
// but returns no marker to continue, the reader must error rather than re-issue
// a marker-less request (which would silently re-fetch the tail forever).
func TestRDSLogReader_PendingWithoutMarkerErrors(t *testing.T) {
	data := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(true)}, // pending, but no Marker
		},
	}
	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("expected an error when RDS reports data pending but returns no marker to continue")
	}
	if !strings.Contains(err.Error(), "no marker to continue") {
		t.Errorf("error = %v, want a 'no marker to continue' message", err)
	}
}

// TestRDSLogReader_PendingNonAdvancingMarkerErrors: an empty page that keeps
// reporting pending with an unchanged marker must break rather than loop.
func TestRDSLogReader_PendingNonAdvancingMarkerErrors(t *testing.T) {
	empty := ""
	stuck := "stuck"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &empty, AdditionalDataPending: boolPtr(true), Marker: &stuck},
			{LogFileData: &empty, AdditionalDataPending: boolPtr(true), Marker: &stuck},
		},
	}
	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("expected an error when an empty page reports pending without advancing the marker")
	}
	if !strings.Contains(err.Error(), "without advancing the marker") {
		t.Errorf("error = %v, want a non-advancing-marker message", err)
	}
}

func TestRDSLogReader_EmptyFile(t *testing.T) {
	empty := ""
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &empty, AdditionalDataPending: boolPtr(false)},
		},
	}

	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty, got %d bytes", len(got))
	}
}

// TestRDSLogReader_ParseMariaDBIntegration streams two pages with a record
// split across the page boundary through the MariaDB parser — the resume
// must neither duplicate nor lose the split record.
func TestRDSLogReader_ParseMariaDBIntegration(t *testing.T) {
	page1 := "20260413 12:00:00,ip-10-3-0-74,admin,172.31.75.212,16,100,QUERY,sbtest,'SELECT c FROM sbtest1 WHERE id=1',0,,\n" +
		"20260413 12:00:01,ip-10-3-0-74,admin,172.31.75.212,16,101,QUE"
	page2 := "RY,sbtest,'UPDATE sbtest1 SET k=k+1 WHERE id=2',0,,\n" +
		"20260413 12:00:02,ip-10-3-0-74,root,172.31.75.212,17,102,CONNECT,,'',0,,\n"
	marker := "p2"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &page1, AdditionalDataPending: boolPtr(true), Marker: &marker},
			{LogFileData: &page2, AdditionalDataPending: boolPtr(false)},
		},
	}

	reader := newRDSLogReader(context.Background(), mock, "test-db", "audit/server_audit.log")
	events, _, skipped, _, err := parseMariaDBFile(reader, auditLogFilter{})
	if err != nil {
		t.Fatalf("parseMariaDBFile: %v", err)
	}
	if skipped != 0 {
		t.Errorf("skipped = %d, want 0", skipped)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[1].EventType != "QUERY" {
		t.Errorf("event[1].EventType = %q, want QUERY", events[1].EventType)
	}
	if !strings.Contains(events[1].SQLText, "UPDATE sbtest1") {
		t.Errorf("event[1].SQLText = %q, want UPDATE sbtest1...", events[1].SQLText)
	}
	if events[2].EventType != "CONNECT" || events[2].User != "root" {
		t.Errorf("event[2] = %+v, want CONNECT by root", events[2])
	}
}

func TestRDSLogReader_ContextCancelled(t *testing.T) {
	data := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	marker := "page2"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(true), Marker: &marker},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	reader := newRDSLogReader(ctx, mock, "test-db", "audit/server_audit.log")

	buf := make([]byte, 4096)
	n, err := reader.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("first Read: n=%d err=%v", n, err)
	}

	cancel()

	if _, err := reader.Read(buf); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled after cancel, got %v", err)
	}
}

// TestRDSLogReader_TailMode_SendsNumberOfLines checks the documented RDS
// tail-fetch shape: NumberOfLines set, Marker omitted, leading partial line
// discarded so downstream parsers see whole records only.
func TestRDSLogReader_TailMode_SendsNumberOfLines(t *testing.T) {
	data := "partial mid-line junk\n" +
		"20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n" +
		"20260413 12:00:01,host,admin,10.0.0.1,42,101,QUERY,mydb,'SELECT 2',0,,\n" +
		"20260413 12:00:02,host,admin,10.0.0.1,42,102,QUERY,mydb,'SELECT 3',0,,\n"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	reader := newRDSLogReaderTail(context.Background(), mock, "test-db", "audit/server_audit.log", 5000)
	events, totalScanned, _, _, err := parseMariaDBFile(reader, auditLogFilter{})
	if err != nil {
		t.Fatalf("parseMariaDBFile: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3 (leading partial line dropped)", len(events))
	}
	if totalScanned != 3 {
		t.Errorf("totalScanned = %d, want 3", totalScanned)
	}

	if len(mock.downloadInputs) != 1 {
		t.Fatalf("expected 1 DownloadDBLogFilePortion call, got %d", len(mock.downloadInputs))
	}
	in := mock.downloadInputs[0]
	if in.NumberOfLines == nil || *in.NumberOfLines != 5000 {
		t.Errorf("NumberOfLines = %v, want 5000", in.NumberOfLines)
	}
	if in.Marker != nil {
		t.Errorf("Marker = %q, want nil (tail-mode omits marker)", *in.Marker)
	}
}

// TestRDSLogReader_TailMode_SinglePage ensures tail mode never paginates
// past the first response even when the server (incorrectly) reports more
// data pending — following the marker would re-fetch downloaded records.
func TestRDSLogReader_TailMode_SinglePage(t *testing.T) {
	page := "partial\n20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	marker := "next"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &page, AdditionalDataPending: boolPtr(true), Marker: &marker},
		},
	}

	reader := newRDSLogReaderTail(context.Background(), mock, "test-db", "audit/server_audit.log", 1000)
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(mock.downloadInputs) != 1 {
		t.Errorf("expected exactly 1 API call (tail-mode is single-page), got %d", len(mock.downloadInputs))
	}
}

// ---------------------------------------------------------------------------
// collectRDSAuditLogFiles
// ---------------------------------------------------------------------------

func TestCollectRDSAuditLogFiles_PrimaryOnly(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/server_audit.log", 1000),
			logFile("audit/server_audit.log.01", 900),
			logFile("audit/server_audit.log.02", 800),
		)},
	}

	files, striped, _, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "/rdsdbdata/log/audit/server_audit.log", false)
	if err != nil {
		t.Fatalf("collectRDSAuditLogFiles: %v", err)
	}
	if striped {
		t.Error("striped = true, want false")
	}
	if len(files) != 1 || files[0] != "audit/server_audit.log" {
		t.Fatalf("files = %v, want [audit/server_audit.log]", files)
	}
}

func TestCollectRDSAuditLogFiles_WithRotated(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/server_audit.log", 1000),
			logFile("audit/server_audit.log.02", 800),
			logFile("audit/server_audit.log.01", 900),
		)},
	}

	files, striped, _, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "/rdsdbdata/log/audit/server_audit.log", true)
	if err != nil {
		t.Fatalf("collectRDSAuditLogFiles: %v", err)
	}
	if striped {
		t.Error("striped = true, want false")
	}
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3", len(files))
	}
	// Primary first, then rotated sorted by LastWritten desc.
	if files[0] != "audit/server_audit.log" || files[1] != "audit/server_audit.log.01" || files[2] != "audit/server_audit.log.02" {
		t.Errorf("files = %v, want primary then newest-first rotated", files)
	}
}

func TestCollectRDSAuditLogFiles_NoFiles(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("error/mysql-error.log", 1000),
		)},
	}

	files, striped, _, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "/rdsdbdata/log/audit/server_audit.log", false)
	if err != nil {
		t.Fatalf("collectRDSAuditLogFiles: %v", err)
	}
	if striped || len(files) != 0 {
		t.Errorf("files = %v striped = %v, want none", files, striped)
	}
}

func TestCollectRDSAuditLogFiles_Paginated(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{
			describePage("page2", logFile("audit/server_audit.log.03", 700)),
			describePage("",
				logFile("audit/server_audit.log", 1000),
				logFile("audit/server_audit.log.01", 900),
			),
		},
	}

	files, _, _, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "/rdsdbdata/log/audit/server_audit.log", true)
	if err != nil {
		t.Fatalf("collectRDSAuditLogFiles: %v", err)
	}
	if mock.describeCalls != 2 {
		t.Errorf("expected 2 DescribeDBLogFiles calls, got %d", mock.describeCalls)
	}
	if len(files) != 3 || files[0] != "audit/server_audit.log" {
		t.Fatalf("files = %v, want primary + 2 rotated", files)
	}
}

// TestCollectRDSAuditLogFiles_AuroraStriped: no single primary matches the
// Aurora audit file layout, so the whole set is returned newest-first with
// striped=true regardless of includeRotated.
func TestCollectRDSAuditLogFiles_AuroraStriped(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/audit.log.0.2026-04-13-11-00.1", 800),
			logFile("audit/audit.log.1.2026-04-13-12-00.0", 1000),
			logFile("audit/audit.log.0.2026-04-13-12-00.0", 900),
		)},
	}

	files, striped, warns, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "", false)
	if err != nil {
		t.Fatalf("collectRDSAuditLogFiles: %v", err)
	}
	if !striped {
		t.Fatal("striped = false, want true")
	}
	want := []string{
		"audit/audit.log.1.2026-04-13-12-00.0",
		"audit/audit.log.0.2026-04-13-12-00.0",
		"audit/audit.log.0.2026-04-13-11-00.1",
	}
	if len(files) != 3 || files[0] != want[0] || files[1] != want[1] || files[2] != want[2] {
		t.Errorf("files = %v, want %v (newest-first)", files, want)
	}
	if len(warns) == 0 || !strings.Contains(warns[0], "striped") {
		t.Errorf("warnings = %v, want a striped-set note", warns)
	}
}

func TestCollectRDSAuditLogFiles_AccessDenied(t *testing.T) {
	mock := &mockRDSClient{
		describeErr: &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"},
	}

	_, _, _, err := collectRDSAuditLogFiles(context.Background(), mock, "test-db", "us-west-2", "/rdsdbdata/log/audit/server_audit.log", false)
	if !errors.Is(err, ErrAuditAccessDenied) {
		t.Fatalf("err = %v, want ErrAuditAccessDenied", err)
	}
	if !strings.Contains(err.Error(), "rds:DescribeDBLogFiles") {
		t.Errorf("error should mention the missing IAM action: %v", err)
	}
}

// ---------------------------------------------------------------------------
// classifyRDSError
// ---------------------------------------------------------------------------

func TestClassifyRDSError_AccessDenied(t *testing.T) {
	err := classifyRDSError(&smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"}, "mydb", "us-west-2")
	if !errors.Is(err, ErrAuditAccessDenied) {
		t.Fatalf("err = %v, want ErrAuditAccessDenied", err)
	}
	if !strings.Contains(err.Error(), "rds:DescribeDBLogFiles") || !strings.Contains(err.Error(), "rds:DownloadDBLogFilePortion") {
		t.Errorf("message should list the required IAM actions: %v", err)
	}
	if !strings.Contains(err.Error(), "arn:aws:rds:us-west-2:*:db:mydb") {
		t.Errorf("message should include the resource ARN: %v", err)
	}
}

func TestClassifyRDSError_InstanceNotFound(t *testing.T) {
	err := classifyRDSError(&smithy.GenericAPIError{Code: "DBInstanceNotFoundFault", Message: "not found"}, "mydb", "us-west-2")
	if !errors.Is(err, ErrAuditRDSInstanceNotFound) {
		t.Fatalf("err = %v, want ErrAuditRDSInstanceNotFound", err)
	}
	// The Aurora cluster-id-vs-instance-id confusion must be called out.
	if !strings.Contains(err.Error(), "cluster") {
		t.Errorf("message should call out the Aurora cluster/instance id confusion: %v", err)
	}
}

func TestClassifyRDSError_Throttling(t *testing.T) {
	err := classifyRDSError(&smithy.GenericAPIError{Code: "Throttling", Message: "rate exceeded"}, "mydb", "us-west-2")
	if !errors.Is(err, ErrAuditThrottled) {
		t.Fatalf("err = %v, want ErrAuditThrottled", err)
	}
}

func TestClassifyRDSError_NoCredentials(t *testing.T) {
	err := classifyRDSError(fmt.Errorf("no valid providers in chain: no EC2 IMDS role found"), "mydb", "us-west-2")
	if !errors.Is(err, ErrAuditNoAWSCredentials) {
		t.Fatalf("err = %v, want ErrAuditNoAWSCredentials", err)
	}
}

func TestClassifyRDSError_Generic(t *testing.T) {
	cause := fmt.Errorf("some random network error")
	err := classifyRDSError(cause, "mydb", "us-west-2")
	for _, sentinel := range []error{ErrAuditAccessDenied, ErrAuditThrottled, ErrAuditRDSInstanceNotFound, ErrAuditNoAWSCredentials} {
		if errors.Is(err, sentinel) {
			t.Errorf("generic error must not match sentinel %v", sentinel)
		}
	}
	if !errors.Is(err, cause) {
		t.Errorf("generic classification should wrap the cause, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// parseRDSAuditLogFiles
// ---------------------------------------------------------------------------

func TestParseRDSAuditLogFiles_WithFilter(t *testing.T) {
	data := "20260413 12:00:00,host,admin,172.31.1.1,42,100,QUERY,sbtest,'SELECT 1',0,,\n" +
		"20260413 12:00:01,host,root,172.31.1.1,43,101,QUERY,mydb,'INSERT INTO t VALUES(1)',0,,\n" +
		"20260413 12:00:02,host,admin,172.31.1.1,42,102,CONNECT,,'',0,,\n"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	filter := auditLogFilter{user: "admin"}
	result, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log"},
		AuditFormatMariaDB, filter, 0, 100, 0, false,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if result.totalScanned != 3 {
		t.Errorf("totalScanned = %d, want 3", result.totalScanned)
	}
	if len(result.events) != 2 {
		t.Fatalf("got %d events, want 2 (admin only)", len(result.events))
	}
	for _, ev := range result.events {
		if ev.User != "admin" {
			t.Errorf("unexpected user %q in filtered results", ev.User)
		}
	}
}

func TestParseRDSAuditLogFiles_Limit(t *testing.T) {
	data := "20260413 12:00:00,host,admin,172.31.1.1,42,100,QUERY,sbtest,'SELECT 1',0,,\n" +
		"20260413 12:00:01,host,admin,172.31.1.1,42,101,QUERY,sbtest,'SELECT 2',0,,\n" +
		"20260413 12:00:02,host,admin,172.31.1.1,42,102,QUERY,sbtest,'SELECT 3',0,,\n"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	result, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log"},
		AuditFormatMariaDB, auditLogFilter{}, 0, 2, 0, false,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if len(result.events) != 2 {
		t.Errorf("got %d events, want 2 (limit)", len(result.events))
	}
}

func TestParseRDSAuditLogFiles_Offset(t *testing.T) {
	data := "20260413 12:00:00,host,admin,172.31.1.1,42,100,QUERY,sbtest,'SELECT 1',0,,\n" +
		"20260413 12:00:01,host,admin,172.31.1.1,42,101,QUERY,sbtest,'SELECT 2',0,,\n" +
		"20260413 12:00:02,host,admin,172.31.1.1,42,102,QUERY,sbtest,'SELECT 3',0,,\n"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	result, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log"},
		AuditFormatMariaDB, auditLogFilter{}, 2, 10, 0, false,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if len(result.events) != 1 || !strings.Contains(result.events[0].SQLText, "SELECT 3") {
		t.Errorf("events = %+v, want only SELECT 3 after offset 2", result.events)
	}
}

// TestParseRDSAuditLogFiles_TailLinesParam wires tail mode through the
// orchestrator: the primary file gets the tail fetch, rotated files read
// from the start (they hold older history a tail would miss).
func TestParseRDSAuditLogFiles_TailLinesParam(t *testing.T) {
	primaryData := "partial\n20260413 12:00:05,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	rotatedData := "20260413 11:00:00,host,admin,10.0.0.1,42,200,QUERY,mydb,'OLDER',0,,\n"

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &primaryData, AdditionalDataPending: boolPtr(false)},
			{LogFileData: &rotatedData, AdditionalDataPending: boolPtr(false)},
		},
	}

	_, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log", "audit/server_audit.log.01"},
		AuditFormatMariaDB, auditLogFilter{}, 0, 100, 5000, false,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if len(mock.downloadInputs) != 2 {
		t.Fatalf("expected 2 API calls, got %d", len(mock.downloadInputs))
	}
	if in := mock.downloadInputs[0]; in.NumberOfLines == nil || *in.NumberOfLines != 5000 {
		t.Errorf("primary file: NumberOfLines = %v, want 5000 (tail mode)", in.NumberOfLines)
	}
	if in := mock.downloadInputs[1]; in.NumberOfLines != nil {
		t.Errorf("rotated file: NumberOfLines = %v, want nil (full scan)", *in.NumberOfLines)
	}
}

// TestParseRDSAuditLogFiles_TailMissWarning keeps the zero-in-window tail
// warning contract: a tail fetch that scanned records but matched none in
// the requested window must say so instead of silently returning empty.
func TestParseRDSAuditLogFiles_TailMissWarning(t *testing.T) {
	data := "partial\n20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	filter := auditLogFilter{since: time.Date(2026, 4, 13, 13, 0, 0, 0, time.UTC)}
	result, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log"},
		AuditFormatMariaDB, filter, 0, 100, 5000, false,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if len(result.events) != 0 {
		t.Fatalf("events = %d, want 0", len(result.events))
	}
	found := false
	for _, w := range result.warnings {
		if strings.Contains(w, "tail fetch") && strings.Contains(w, "no events in the requested time window") {
			found = true
		}
	}
	if !found {
		t.Errorf("warnings = %v, want a tail-miss warning", result.warnings)
	}
}

// TestParseRDSAuditLogFiles_DownloadErrorIsFatal: API failures mid-stream
// (IAM, throttling, network) abort with a classified error rather than
// demoting to a warning with silently partial data.
func TestParseRDSAuditLogFiles_DownloadErrorIsFatal(t *testing.T) {
	mock := &mockRDSClient{
		downloadErr: &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"},
	}

	_, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/server_audit.log"},
		AuditFormatMariaDB, auditLogFilter{}, 0, 100, 0, false,
	)
	if !errors.Is(err, ErrAuditAccessDenied) {
		t.Fatalf("err = %v, want ErrAuditAccessDenied", err)
	}
}

// TestParseRDSAuditLogFiles_StripedSortAndLimit is the Aurora quirk test:
// records interleave across concurrent files, so all files are parsed
// before sorting by record timestamp and paging. With limit=2 the result
// must be the two EARLIEST events across BOTH files — inline per-file
// paging would instead return both events of the first file.
func TestParseRDSAuditLogFiles_StripedSortAndLimit(t *testing.T) {
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	auroraLine := func(offset time.Duration, id int, q string) string {
		micros := base.Add(offset).UnixMicro()
		return fmt.Sprintf("%d,host,admin,10.0.0.1,42,%d,QUERY,mydb,'%s',0\n", micros, id, q)
	}
	fileA := auroraLine(0, 100, "A1") + auroraLine(60*time.Second, 101, "A2")
	fileB := auroraLine(30*time.Second, 200, "B1") + auroraLine(90*time.Second, 201, "B2")

	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &fileA, AdditionalDataPending: boolPtr(false)},
			{LogFileData: &fileB, AdditionalDataPending: boolPtr(false)},
		},
	}

	result, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/audit.log.0.x", "audit/audit.log.1.x"},
		AuditFormatMariaDB, auditLogFilter{}, 0, 2, 0, true,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if result.totalScanned != 4 {
		t.Errorf("totalScanned = %d, want 4 (all files parsed before paging)", result.totalScanned)
	}
	if len(result.events) != 2 {
		t.Fatalf("got %d events, want 2", len(result.events))
	}
	if result.events[0].SQLText != "A1" || result.events[1].SQLText != "B1" {
		t.Errorf("events = [%s %s], want [A1 B1] (earliest across both files)",
			result.events[0].SQLText, result.events[1].SQLText)
	}
}

// TestParseRDSAuditLogFiles_StripedTailAllFiles: in striped mode every file
// gets the tail fetch (stripes are concurrent — all hold recent records).
func TestParseRDSAuditLogFiles_StripedTailAllFiles(t *testing.T) {
	data := "partial\n20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	mock := &mockRDSClient{
		downloadPages: []*rds.DownloadDBLogFilePortionOutput{
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
			{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
		},
	}

	_, err := parseRDSAuditLogFiles(
		context.Background(), mock, "test-db", "us-west-2",
		[]string{"audit/audit.log.0.x", "audit/audit.log.1.x"},
		AuditFormatMariaDB, auditLogFilter{}, 0, 100, 5000, true,
	)
	if err != nil {
		t.Fatalf("parseRDSAuditLogFiles: %v", err)
	}
	if len(mock.downloadInputs) != 2 {
		t.Fatalf("expected 2 API calls, got %d", len(mock.downloadInputs))
	}
	for i, in := range mock.downloadInputs {
		if in.NumberOfLines == nil || *in.NumberOfLines != 5000 {
			t.Errorf("striped file %d: NumberOfLines = %v, want 5000", i, in.NumberOfLines)
		}
	}
}

// ---------------------------------------------------------------------------
// sortAuditEventsByTimestamp
// ---------------------------------------------------------------------------

func TestSortAuditEventsByTimestamp(t *testing.T) {
	events := []AuditEvent{
		{Timestamp: "2026-04-13T12:02:00Z", SQLText: "third"},
		{Timestamp: "not-a-date", SQLText: "unparseable"},
		{Timestamp: "20260413 12:01:00", SQLText: "second"}, // MariaDB local layout
		{Timestamp: "2026-04-13T12:00:00Z", SQLText: "first"},
	}
	sortAuditEventsByTimestamp(events)
	got := []string{events[0].SQLText, events[1].SQLText, events[2].SQLText, events[3].SQLText}
	want := []string{"unparseable", "first", "second", "third"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// ReadAuditLog dispatch and fallback seams
// ---------------------------------------------------------------------------

const showServerAuditLoggingSQL = "SHOW GLOBAL VARIABLES LIKE 'server_audit_logging'"

func rdsAuditPage() []*rds.DownloadDBLogFilePortionOutput {
	data := "20260413 12:00:00,host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n"
	return []*rds.DownloadDBLogFilePortionOutput{
		{LogFileData: &data, AdditionalDataPending: boolPtr(false)},
	}
}

func TestReadAuditLog_UnsupportedSource(t *testing.T) {
	_, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{Source: AuditSource("bogus")})
	if err == nil || !strings.Contains(err.Error(), "unsupported audit source") {
		t.Fatalf("err = %v, want unsupported-source error", err)
	}
}

func TestReadAuditLog_RDSSourceRequiresRDSHost(t *testing.T) {
	swapRDSFactory(t, nil, nil) // must not even be reached
	_, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceRDS,
		SourceHost: "db.example.com",
	})
	if err == nil || !strings.Contains(err.Error(), "requires an RDS endpoint host") {
		t.Fatalf("err = %v, want RDS-host requirement error", err)
	}
}

// TestReadAuditLog_ExplicitRDSSource: Source=rds with a nil sourceDB (no
// discovery possible) still works — defaults to the audit/ directory and
// the MariaDB dialect, which is what RDS and Aurora always write.
func TestReadAuditLog_ExplicitRDSSource_NilDB(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/server_audit.log", 1000),
		)},
		downloadPages: rdsAuditPage(),
	}
	swapRDSFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceRDS,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com:3306",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Source != AuditSourceRDS {
		t.Errorf("Source = %q, want rds", res.Source)
	}
	if res.FormatDetected != AuditFormatMariaDB || res.Variant != AuditVariantMariaDB {
		t.Errorf("format/variant = %q/%q, want mariadb/mariadb", res.FormatDetected, res.Variant)
	}
	if res.FilePath != "audit/server_audit.log" || res.FilesRead != 1 {
		t.Errorf("FilePath/FilesRead = %q/%d, want audit/server_audit.log / 1", res.FilePath, res.FilesRead)
	}
	if len(res.Events) != 1 || res.Events[0].User != "admin" {
		t.Errorf("Events = %+v, want the single admin event", res.Events)
	}
}

// TestReadAuditLog_ExplicitRDSSource_WithDiscovery: Source=rds uses
// best-effort discovery for the configured path and variant.
func TestReadAuditLog_ExplicitRDSSource_WithDiscovery(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(variableRows("server_audit_file_path", "/rdsdbdata/log/audit/server_audit.log"))

	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/server_audit.log", 1000),
		)},
		downloadPages: rdsAuditPage(),
	}
	swapRDSFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{
		Source:     AuditSourceRDS,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Source != AuditSourceRDS || len(res.Events) != 1 {
		t.Errorf("Source/Events = %q/%d, want rds/1", res.Source, len(res.Events))
	}
	if err := dbmock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestReadAuditLog_AutoFallsBackToRDS: auto mode with a configured audit
// file that is not on the local filesystem and an RDS-shaped host must
// fall back to the RDS file API (the SaaS seam).
func TestReadAuditLog_AutoFallsBackToRDS(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(variableRows("server_audit_file_path", "/rdsdbdata/log/audit/server_audit.log"))

	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/server_audit.log", 1000),
		)},
		downloadPages: rdsAuditPage(),
	}
	swapRDSFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Source != AuditSourceRDS {
		t.Errorf("Source = %q, want rds (fallback)", res.Source)
	}
	if len(res.Events) != 1 {
		t.Errorf("Events = %d, want 1", len(res.Events))
	}
}

// TestReadAuditLog_AutoNoFallbackForNonRDSHost: the same missing local file
// with a non-RDS host keeps the original ErrAuditFileNotFound.
func TestReadAuditLog_AutoNoFallbackForNonRDSHost(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(variableRows("server_audit_file_path", "/rdsdbdata/log/audit/server_audit.log"))

	swapRDSFactory(t, nil, nil) // must not be reached

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{
		SourceHost: "db.internal.example.com",
	})
	if !errors.Is(err, ErrAuditFileNotFound) {
		t.Fatalf("err = %v, want ErrAuditFileNotFound", err)
	}
}

// TestReadAuditLog_LocalSourceNeverFallsBack: Source=local pins the local
// filesystem even for RDS hosts.
func TestReadAuditLog_LocalSourceNeverFallsBack(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(variableRows("server_audit_file_path", "/rdsdbdata/log/audit/server_audit.log"))

	swapRDSFactory(t, nil, nil) // must not be reached

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{
		Source:     AuditSourceLocal,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if !errors.Is(err, ErrAuditFileNotFound) {
		t.Fatalf("err = %v, want ErrAuditFileNotFound", err)
	}
}

// TestReadAuditLog_AuroraNotConfiguredFallback: Aurora Advanced Auditing
// exposes no file-path variable — auto mode probes server_audit_logging and
// falls back to the RDS API when it is ON.
func TestReadAuditLog_AuroraNotConfiguredFallback(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditLoggingSQL).WillReturnRows(variableRows("server_audit_logging", "ON"))

	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("",
			logFile("audit/audit.log.0.2026-04-13-12-00.0", 1000),
			logFile("audit/audit.log.1.2026-04-13-12-00.0", 900),
		)},
		downloadPages: rdsAuditPage(),
	}
	swapRDSFactory(t, mock, nil)

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{
		SourceHost: "myinstance.abc123.us-west-2.rds.amazonaws.com",
	})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Source != AuditSourceRDS || res.Variant != AuditVariantMariaDB {
		t.Errorf("Source/Variant = %q/%q, want rds/mariadb", res.Source, res.Variant)
	}
	if res.FilesRead != 2 {
		t.Errorf("FilesRead = %d, want 2 (striped set)", res.FilesRead)
	}
	if err := dbmock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestReadAuditLog_NotConfiguredWithoutAuditLogging: no audit variables and
// server_audit_logging not ON keeps ErrAuditNotConfigured — no AWS calls.
func TestReadAuditLog_NotConfiguredWithoutAuditLogging(t *testing.T) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	dbmock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(noVariableRows())
	dbmock.ExpectQuery(showServerAuditLoggingSQL).WillReturnRows(noVariableRows())

	swapRDSFactory(t, nil, nil) // must not be reached

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{
		SourceHost: "myinstance.abc123.us-west-2.rds.amazonaws.com",
	})
	if !errors.Is(err, ErrAuditNotConfigured) {
		t.Fatalf("err = %v, want ErrAuditNotConfigured", err)
	}
}

// TestReadAuditLog_RDSNoFilesFound: an RDS read that finds no audit files
// wraps ErrAuditFileNotFound with remediation.
func TestReadAuditLog_RDSNoFilesFound(t *testing.T) {
	mock := &mockRDSClient{
		describePages: []*rds.DescribeDBLogFilesOutput{describePage("")},
	}
	swapRDSFactory(t, mock, nil)

	_, err := ReadAuditLog(context.Background(), nil, AuditReadOptions{
		Source:     AuditSourceRDS,
		SourceHost: "mydb.abc123.us-west-2.rds.amazonaws.com",
	})
	if !errors.Is(err, ErrAuditFileNotFound) {
		t.Fatalf("err = %v, want ErrAuditFileNotFound", err)
	}
	if !strings.Contains(err.Error(), "server_audit_logging") {
		t.Errorf("error should carry remediation, got: %v", err)
	}
}
