// RDS file-API audit log source (port of the SaaS agent's rds_audit_log.go).
//
// When bintrail runs off-host against an RDS MySQL/MariaDB instance (or an
// Aurora MySQL cluster), the audit log file reported by the server lives on
// AWS-managed storage and is unreachable via the local filesystem. This
// source downloads it through the RDS API instead: DescribeDBLogFiles
// enumerates the audit files and DownloadDBLogFilePortion streams their
// contents (Marker-paginated, ~1 MB per call).
//
// Required IAM actions (scope to the instance ARN in production):
//   - rds:DescribeDBLogFiles
//   - rds:DownloadDBLogFilePortion
//
// AWS recommends the CloudWatch Logs export (auditlog_cloudwatch.go) over
// this API for sustained use: DownloadDBLogFilePortion is served by the DB
// instance itself and consumes its resources.
//
// Aurora quirk: each Aurora MySQL instance stripes audit records across
// four concurrent files (rotated at 100 MB) with no ordering guarantee
// across files. When no single primary audit file matches, the whole set is
// treated as striped: every file is downloaded and events are re-sorted by
// their record-embedded timestamps after parsing.
package forensics

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	smithy "github.com/aws/smithy-go"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Sentinel errors for the remote (AWS) audit log sources. Wrapped with
// remediation context at the failure site; callers errors.Is on these.
var (
	// ErrAuditAccessDenied means the AWS credentials lack an IAM action the
	// source needs (rds:DescribeDBLogFiles / rds:DownloadDBLogFilePortion
	// for the RDS source, logs:FilterLogEvents for CloudWatch).
	ErrAuditAccessDenied = errors.New("insufficient IAM permissions to read the audit log")
	// ErrAuditThrottled means an AWS API rate limit was hit; the request is
	// retryable after a short backoff.
	ErrAuditThrottled = errors.New("AWS API rate limit exceeded while reading the audit log")
	// ErrAuditRDSInstanceNotFound means the DB instance identifier derived
	// from the DSN hostname does not exist in the region — commonly the
	// Aurora cluster-id-vs-instance-id confusion.
	ErrAuditRDSInstanceNotFound = errors.New("RDS instance not found")
	// ErrAuditNoAWSCredentials means no AWS credentials could be resolved
	// from the default chain (env, shared config, IMDS role).
	ErrAuditNoAWSCredentials = errors.New("AWS credentials are not available")
)

// rdsLogAPI is the subset of the RDS client used for audit log download.
// Narrow interface for testability, following the storage.s3API pattern.
type rdsLogAPI interface {
	DescribeDBLogFiles(ctx context.Context, params *rds.DescribeDBLogFilesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error)
	DownloadDBLogFilePortion(ctx context.Context, params *rds.DownloadDBLogFilePortionInput, optFns ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error)
}

// newRDSLogClient builds the real RDS client. Package-level hook so unit
// tests can inject a mock without AWS credentials. All config/region
// resolution goes through storage.LoadAWSConfig (EC2/ECS IMDS region
// fallback, #697) — never awsconfig.LoadDefaultConfig directly.
var newRDSLogClient = func(ctx context.Context, region string) (rdsLogAPI, error) {
	cfg, err := storage.LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	return rds.NewFromConfig(cfg), nil
}

// ---------------------------------------------------------------------------
// RDS hostname parsing
// ---------------------------------------------------------------------------

// rdsHostRe matches RDS endpoint hostnames:
//
//	<db-identifier>.<hash>.<region>.rds.amazonaws.com
//
// Aurora cluster endpoints match too — their middle label carries a
// "cluster-" prefix (see isRDSClusterEndpoint).
var rdsHostRe = regexp.MustCompile(`^([^.]+)\.[^.]+\.([^.]+)\.rds\.amazonaws\.com$`)

// parseRDSHost extracts the DB identifier and region from an RDS endpoint
// hostname. Returns ("", "", false) if the hostname is not RDS-shaped.
func parseRDSHost(hostname string) (dbIdentifier, region string, ok bool) {
	m := rdsHostRe.FindStringSubmatch(hostname)
	if len(m) < 3 {
		return "", "", false
	}
	return m[1], m[2], true
}

// isRDSClusterEndpoint reports whether an RDS-shaped hostname is an Aurora
// cluster (or cluster reader) endpoint: its middle label starts with
// "cluster-" (e.g. mydb.cluster-abc123.us-west-2.rds.amazonaws.com). The
// identifier of such a host is a CLUSTER id, not an instance id.
func isRDSClusterEndpoint(hostname string) bool {
	labels := strings.Split(hostname, ".")
	return len(labels) > 2 && strings.HasPrefix(labels[1], "cluster-")
}

// normalizeAuditHost lowercases a DSN host and strips a :port suffix so it
// can be matched against the RDS endpoint pattern.
func normalizeAuditHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// rdsHostCandidate reports whether opts.SourceHost looks like an RDS
// endpoint — the gate for the automatic local→RDS fallback.
func rdsHostCandidate(host string) bool {
	_, _, ok := parseRDSHost(normalizeAuditHost(host))
	return ok
}

// serverAuditLoggingOn reports whether the server_audit_logging global
// variable is ON. Aurora Advanced Auditing exposes no file-path variable,
// so this is the only signal that auditing is active there.
func serverAuditLoggingOn(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var name, value string
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'server_audit_logging'").Scan(&name, &value); err != nil {
		return false
	}
	return strings.EqualFold(value, "ON") || value == "1"
}

// ---------------------------------------------------------------------------
// RDS log file collection
// ---------------------------------------------------------------------------

// rdsLogPrefix is the filesystem prefix RDS uses internally for log paths.
// The DescribeDBLogFiles API returns paths relative to /rdsdbdata/log/.
const rdsLogPrefix = "/rdsdbdata/log/"

// collectRDSAuditLogFiles lists the audit log files available on an RDS
// instance, mapping the local path reported by MySQL variables (e.g.
// "/rdsdbdata/log/audit/server_audit.log") to the RDS API path format
// ("audit/server_audit.log").
//
// striped is true when no single primary file matched but audit files exist
// under the directory prefix — the Aurora layout, where each instance
// writes records across multiple concurrent files. In that case files holds
// the whole set (newest-first by LastWritten, capped) and the caller must
// re-sort parsed events by timestamp.
func collectRDSAuditLogFiles(
	ctx context.Context,
	client rdsLogAPI,
	dbIdentifier string,
	region string,
	localAuditPath string,
	includeRotated bool,
) (files []string, striped bool, warnings []string, err error) {
	// Map local path to RDS API path.
	apiPath := strings.TrimPrefix(localAuditPath, rdsLogPrefix)
	// If the path is still absolute or empty, fall back to the default
	// audit directory.
	if apiPath == "" || apiPath[0] == '/' {
		apiPath = "audit/"
	}

	// When the configured path is a directory ("audit/" or "audit" without
	// trailing slash), append the default MariaDB audit log filename.
	// Directories are detected by a trailing slash OR the absence of a file
	// extension in the last segment.
	if strings.HasSuffix(apiPath, "/") || !strings.Contains(lastSegment(apiPath), ".") {
		apiPath = strings.TrimRight(apiPath, "/") + "/server_audit.log"
	}

	// Strip the filename to get the directory prefix for filtering.
	dirPrefix := apiPath
	if idx := strings.LastIndex(dirPrefix, "/"); idx >= 0 {
		dirPrefix = dirPrefix[:idx+1] // "audit/"
	}

	input := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: &dbIdentifier,
		FilenameContains:     &dirPrefix,
	}

	// Paginate — the API returns at most 100 entries per call.
	var all []rdstypes.DescribeDBLogFilesDetails
	for {
		resp, derr := client.DescribeDBLogFiles(ctx, input)
		if derr != nil {
			return nil, false, warnings, classifyRDSError(derr, dbIdentifier, region)
		}
		all = append(all, resp.DescribeDBLogFiles...)
		if resp.Marker == nil || *resp.Marker == "" {
			break
		}
		input.Marker = resp.Marker
	}

	// Find the primary file and optionally its rotated siblings.
	var primary string
	var rotated []rdstypes.DescribeDBLogFilesDetails
	for _, f := range all {
		if f.LogFileName == nil {
			continue
		}
		name := *f.LogFileName
		if name == apiPath {
			primary = name
		} else if includeRotated && strings.HasPrefix(name, apiPath+".") {
			rotated = append(rotated, f)
		}
	}

	// No exact match — try matching by basename.
	if primary == "" {
		for _, f := range all {
			if f.LogFileName == nil {
				continue
			}
			name := *f.LogFileName
			if !strings.Contains(name, ".") {
				continue
			}
			if strings.HasSuffix(name, "/"+lastSegment(apiPath)) || name == lastSegment(apiPath) {
				primary = name
				break
			}
		}
	}

	if primary != "" {
		files = []string{primary}
		sortByLastWrittenDesc(rotated)
		for i, f := range rotated {
			if i >= maxRotatedFiles {
				break
			}
			if f.LogFileName != nil {
				files = append(files, *f.LogFileName)
			}
		}
		return files, false, warnings, nil
	}

	// Striped fallback (Aurora): no single primary, but the audit directory
	// holds files — treat every file under the prefix as part of one
	// concurrent set. FilenameContains already filtered server-side; the
	// prefix check repeats it defensively.
	var candidates []rdstypes.DescribeDBLogFilesDetails
	for _, f := range all {
		if f.LogFileName != nil && strings.HasPrefix(*f.LogFileName, dirPrefix) {
			candidates = append(candidates, f)
		}
	}
	if len(candidates) == 0 {
		return nil, false, warnings, nil // no audit log files at all
	}
	sortByLastWrittenDesc(candidates)
	for i, f := range candidates {
		if i > maxRotatedFiles { // primary slot + maxRotatedFiles
			break
		}
		files = append(files, *f.LogFileName)
	}
	warnings = append(warnings, fmt.Sprintf(
		"no single audit log file matched %q; reading %d files under %q as a striped set "+
			"(Aurora writes each instance's audit records across concurrent files; events are re-sorted "+
			"by record timestamp after parsing — consider the cloudwatch source for large time windows)",
		apiPath, len(files), dirPrefix))
	return files, true, warnings, nil
}

// lastSegment returns the last path segment (after the final slash).
func lastSegment(path string) string {
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}

// sortByLastWrittenDesc orders log file entries newest-first.
func sortByLastWrittenDesc(files []rdstypes.DescribeDBLogFilesDetails) {
	lastWritten := func(f rdstypes.DescribeDBLogFilesDetails) int64 {
		if f.LastWritten == nil {
			return 0
		}
		return *f.LastWritten
	}
	slices.SortStableFunc(files, func(a, b rdstypes.DescribeDBLogFilesDetails) int {
		return cmp.Compare(lastWritten(b), lastWritten(a))
	})
}

// ---------------------------------------------------------------------------
// rdsLogReader — io.Reader over paginated DownloadDBLogFilePortion
// ---------------------------------------------------------------------------

// rdsLogReader implements io.Reader by streaming data from the RDS
// DownloadDBLogFilePortion API. Each Read serves buffered bytes; when the
// buffer is exhausted it fetches the next ~1 MB page via Marker until
// AdditionalDataPending is false. The line-oriented parsers sit on top of a
// bufio.Scanner, which reassembles records split across page boundaries, so
// resumed pages never duplicate or lose lines.
//
// Tail mode: when tailLines > 0 the first API call sets NumberOfLines and
// omits Marker, which per the RDS API contract returns the most recent
// lines from the end of the file. The leading partial line (the fetch
// usually lands mid-line) is discarded, and the reader terminates after
// that single response — following the response Marker after a tail fetch
// would re-download data we already have.
type rdsLogReader struct {
	ctx          context.Context
	client       rdsLogAPI
	dbIdentifier string
	logFileName  string
	marker       *string
	buf          []byte
	done         bool
	err          error // sticky API error

	// tailLines, when > 0, triggers tail mode on the first API call and
	// terminates the reader after that single response.
	tailLines int32
	// firstPage tracks whether the first API call has been made — it
	// distinguishes "no marker yet because tail-mode first call" from "no
	// marker yet because paginated read from the start".
	firstPage bool
	// tailStripped tracks whether the partial leading line of a tail fetch
	// has been discarded.
	tailStripped bool
}

func newRDSLogReader(ctx context.Context, client rdsLogAPI, dbIdentifier, logFileName string) *rdsLogReader {
	return &rdsLogReader{
		ctx:          ctx,
		client:       client,
		dbIdentifier: dbIdentifier,
		logFileName:  logFileName,
	}
}

// newRDSLogReaderTail returns a reader that fetches only the last tailLines
// lines of the log file and then stops.
func newRDSLogReaderTail(ctx context.Context, client rdsLogAPI, dbIdentifier, logFileName string, tailLines int32) *rdsLogReader {
	r := newRDSLogReader(ctx, client, dbIdentifier, logFileName)
	if tailLines > 0 {
		r.tailLines = tailLines
	}
	return r
}

func (r *rdsLogReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.err != nil {
		return 0, r.err
	}

	// Serve from the buffer first.
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}

	// Fetch the next page.
	input := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &r.dbIdentifier,
		LogFileName:          &r.logFileName,
	}
	// Tail mode on the first call: set NumberOfLines, leave Marker nil.
	tailFirstCall := !r.firstPage && r.tailLines > 0
	if tailFirstCall {
		n := r.tailLines
		input.NumberOfLines = &n
	} else if r.marker != nil {
		input.Marker = r.marker
	}
	r.firstPage = true

	resp, err := r.client.DownloadDBLogFilePortion(r.ctx, input)
	if err != nil {
		r.err = err
		return 0, err
	}

	if resp.LogFileData == nil || *resp.LogFileData == "" {
		r.done = true
		return 0, io.EOF
	}

	data := *resp.LogFileData
	// A tail-mode fetch typically lands mid-line. Discard everything up to
	// (and including) the first newline so downstream parsers see whole
	// records only.
	if tailFirstCall && !r.tailStripped {
		if idx := strings.IndexByte(data, '\n'); idx >= 0 {
			data = data[idx+1:]
		}
		r.tailStripped = true
	}

	r.buf = []byte(data)
	r.marker = resp.Marker
	if resp.AdditionalDataPending == nil || !*resp.AdditionalDataPending {
		r.done = true
	}
	// Tail mode is single-page — treating subsequent markers as "forward
	// from the end" would duplicate the records already fetched.
	if tailFirstCall {
		r.done = true
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// ---------------------------------------------------------------------------
// Format from variant (the RDS path cannot peek at file bytes)
// ---------------------------------------------------------------------------

// formatFromVariant maps the audit plugin variant to the log format. Used
// on remote paths where detectAuditLogFormat's byte-peek is unavailable.
func formatFromVariant(variant AuditVariant) AuditFormat {
	switch variant {
	case AuditVariantMariaDB:
		return AuditFormatMariaDB
	case AuditVariantPercona:
		return AuditFormatCSV
	case AuditVariantMySQLEnterprise:
		return AuditFormatJSON
	default:
		return AuditFormatUnknown
	}
}

// ---------------------------------------------------------------------------
// AWS error classification
// ---------------------------------------------------------------------------

// classifyRDSError maps AWS SDK errors from the RDS log APIs to the
// package's sentinel errors with actionable remediation.
func classifyRDSError(err error, dbIdentifier, region string) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch {
		case code == "AccessDenied" || code == "AccessDeniedException" ||
			code == "UnauthorizedOperation":
			return fmt.Errorf(
				"%w: add rds:DescribeDBLogFiles and rds:DownloadDBLogFilePortion to the IAM policy "+
					"(resource: arn:aws:rds:%s:*:db:%s)",
				ErrAuditAccessDenied, region, dbIdentifier)
		case code == "Throttling" || code == "ThrottlingException" ||
			code == "RequestLimitExceeded" || code == "TooManyRequestsException":
			return fmt.Errorf(
				"%w: RDS API throttled while downloading audit logs for instance %q; retry after a few seconds",
				ErrAuditThrottled, dbIdentifier)
		case code == "DBInstanceNotFound" || code == "DBInstanceNotFoundFault":
			return fmt.Errorf(
				"%w: %q — the identifier extracted from the hostname may be wrong: Aurora cluster "+
					"endpoints carry the CLUSTER id, but the RDS log-file API takes an INSTANCE id. "+
					"Point the DSN at an instance endpoint, or use the cloudwatch source (which reads "+
					"the cluster-level log group)",
				ErrAuditRDSInstanceNotFound, dbIdentifier)
		default:
			return fmt.Errorf("RDS audit log download failed: %s (%s)", apiErr.ErrorMessage(), code)
		}
	}

	if isAWSCredentialError(err) {
		return fmt.Errorf(
			"%w: attach an IAM role or configure credentials with rds:DescribeDBLogFiles and "+
				"rds:DownloadDBLogFilePortion to download audit logs from RDS",
			ErrAuditNoAWSCredentials)
	}

	return fmt.Errorf("RDS audit log download failed: %w", err)
}

// isAWSCredentialError detects credential-chain resolution failures, which
// the SDK reports as plain errors rather than modeled API errors.
func isAWSCredentialError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "no EC2 IMDS") ||
		strings.Contains(msg, "failed to refresh cached credentials") ||
		strings.Contains(msg, "no valid providers")
}

// ---------------------------------------------------------------------------
// RDS audit log parsing orchestrator
// ---------------------------------------------------------------------------

// rdsFileParse is the outcome of downloading and parsing one RDS log file.
type rdsFileParse struct {
	events       []AuditEvent
	totalScanned int
	skipped      int
	notes        []string // deduplicated into warnings (e.g. local-time caveat)
	warnings     []string // appended verbatim (parse errors)
	err          error    // fatal, classified API error
}

// parseOneRDSLogFile downloads one log file through the API and parses it.
// tailLines > 0 fetches only the file's tail.
func parseOneRDSLogFile(
	ctx context.Context,
	client rdsLogAPI,
	dbIdentifier, region, logFile string,
	format AuditFormat,
	filter auditLogFilter,
	tailLines int32,
) rdsFileParse {
	var reader *rdsLogReader
	if tailLines > 0 {
		reader = newRDSLogReaderTail(ctx, client, dbIdentifier, logFile, tailLines)
	} else {
		reader = newRDSLogReader(ctx, client, dbIdentifier, logFile)
	}

	var out rdsFileParse
	var parseErr error
	switch format {
	case AuditFormatJSON:
		out.events, out.totalScanned, out.skipped, parseErr = parseAuditJSON(reader, filter)
	case AuditFormatXML:
		out.events, out.totalScanned, out.skipped, parseErr = parseAuditXML(reader, filter)
	case AuditFormatCSV:
		out.events, out.totalScanned, out.skipped, parseErr = parsePerconaCSV(reader, filter)
	case AuditFormatMariaDB:
		out.events, out.totalScanned, out.skipped, out.notes, parseErr = parseMariaDBFile(reader, filter)
	default:
		out.err = fmt.Errorf("unsupported audit log format: %s", format)
		return out
	}

	if parseErr != nil {
		// API errors (IAM, throttling, network) surface through the reader
		// and are fatal; parse errors in downloaded data are non-fatal
		// warnings alongside the events collected so far.
		if reader.err != nil {
			out.err = fmt.Errorf("RDS download failed for %s: %w", logFile, classifyRDSError(reader.err, dbIdentifier, region))
			return out
		}
		out.warnings = append(out.warnings, fmt.Sprintf("parse error in %s: %v", logFile, parseErr))
	}
	return out
}

// parseRDSAuditLogFiles downloads and parses audit log files via the RDS
// API. It mirrors parseAuditLogFiles but builds rdsLogReader streams
// instead of opening local files.
//
// Classic layout (striped=false): tail mode applies to the primary file
// only (rotated files hold older history a tail-scan would miss), matched
// events are paged inline (offset/limit), and file downloads stop early
// once the limit is reached.
//
// Striped layout (striped=true, Aurora): records interleave across
// concurrent files, so every file is parsed (tail mode, when set, applies
// to each), all matched events are re-sorted by their record-embedded
// timestamps, and offset/limit page the sorted result. Total accumulation
// is capped at maxEventsPerFile.
func parseRDSAuditLogFiles(
	ctx context.Context,
	client rdsLogAPI,
	dbIdentifier string,
	region string,
	logFiles []string,
	format AuditFormat,
	filter auditLogFilter,
	offset, limit int,
	tailLines int32,
	striped bool,
) (*parseResult, error) {
	if striped {
		return parseStripedRDSAuditLogFiles(ctx, client, dbIdentifier, region, logFiles, format, filter, offset, limit, tailLines)
	}

	res := &parseResult{}
	matched := 0
	for idx, logFile := range logFiles {
		if len(res.events) >= limit {
			break
		}
		tl := int32(0)
		if idx == 0 {
			tl = tailLines
		}
		fr := parseOneRDSLogFile(ctx, client, dbIdentifier, region, logFile, format, filter, tl)
		res.totalScanned += fr.totalScanned
		res.skippedLines += fr.skipped
		res.warnings = appendUniqueWarnings(res.warnings, fr.notes)
		res.warnings = append(res.warnings, fr.warnings...)
		if fr.err != nil {
			return res, fr.err
		}

		// Tail-mode contract: a tail fetch that yields no in-window events
		// means the requested window likely extends beyond the tail —
		// surface it rather than silently returning empty results.
		if idx == 0 && tailLines > 0 && !filter.since.IsZero() && len(fr.events) == 0 && (fr.totalScanned > 0 || fr.skipped > 0) {
			res.warnings = append(res.warnings, fmt.Sprintf(
				"tail fetch (%d lines) returned no events in the requested time window; the audit log window "+
					"may extend beyond the tail — retry with a larger tail_lines, tail_lines=-1 for a full scan, "+
					"or include_rotated=true to search rotated files",
				tailLines))
		}

		for i := range fr.events {
			matched++
			if matched <= offset {
				continue
			}
			res.events = append(res.events, fr.events[i])
			if len(res.events) >= limit {
				break
			}
		}
	}
	return res, nil
}

// parseStripedRDSAuditLogFiles handles the Aurora striped layout: parse
// everything, sort by record timestamp, then page.
func parseStripedRDSAuditLogFiles(
	ctx context.Context,
	client rdsLogAPI,
	dbIdentifier string,
	region string,
	logFiles []string,
	format AuditFormat,
	filter auditLogFilter,
	offset, limit int,
	tailLines int32,
) (*parseResult, error) {
	res := &parseResult{}
	var all []AuditEvent
	anyScanned := false

	for _, logFile := range logFiles {
		fr := parseOneRDSLogFile(ctx, client, dbIdentifier, region, logFile, format, filter, tailLines)
		res.totalScanned += fr.totalScanned
		res.skippedLines += fr.skipped
		res.warnings = appendUniqueWarnings(res.warnings, fr.notes)
		res.warnings = append(res.warnings, fr.warnings...)
		if fr.err != nil {
			return res, fr.err
		}
		anyScanned = anyScanned || fr.totalScanned > 0 || fr.skipped > 0
		all = append(all, fr.events...)
		if len(all) >= maxEventsPerFile {
			all = all[:maxEventsPerFile]
			res.warnings = append(res.warnings, fmt.Sprintf(
				"matched-event cap (%d) reached across the striped file set; narrow since/until", maxEventsPerFile))
			break
		}
	}

	if tailLines > 0 && !filter.since.IsZero() && len(all) == 0 && anyScanned {
		res.warnings = append(res.warnings, fmt.Sprintf(
			"tail fetch (%d lines per file) returned no events in the requested time window across the "+
				"striped file set; retry with a larger tail_lines or tail_lines=-1 for a full scan",
			tailLines))
	}

	sortAuditEventsByTimestamp(all)
	if offset > 0 {
		if offset >= len(all) {
			all = nil
		} else {
			all = all[offset:]
		}
	}
	if len(all) > limit {
		all = all[:limit]
	}
	res.events = all
	return res, nil
}

// sortAuditEventsByTimestamp stably sorts events ascending by their
// record-embedded timestamp. Events with unparseable timestamps sort as
// time zero but keep their relative order.
func sortAuditEventsByTimestamp(events []AuditEvent) {
	if len(events) < 2 {
		return
	}
	type tsEvent struct {
		ts time.Time
		ev AuditEvent
	}
	tagged := make([]tsEvent, len(events))
	for i := range events {
		ts, err := parseFlexTimestamp(events[i].Timestamp)
		if err != nil {
			ts = time.Time{}
		}
		tagged[i] = tsEvent{ts: ts, ev: events[i]}
	}
	slices.SortStableFunc(tagged, func(a, b tsEvent) int {
		return a.ts.Compare(b.ts)
	})
	for i := range tagged {
		events[i] = tagged[i].ev
	}
}

// appendUniqueWarnings appends each note to warnings unless already present
// (parser notes like the MariaDB local-time caveat repeat per file).
func appendUniqueWarnings(warnings, notes []string) []string {
	for _, note := range notes {
		if !slices.Contains(warnings, note) {
			warnings = append(warnings, note)
		}
	}
	return warnings
}

// clampTailLines converts the resolved tail-lines value to the int32 the
// RDS API takes.
func clampTailLines(n int) int32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

// readAuditRDSExplicit serves AuditSourceRDS: discovery is best-effort
// (Aurora exposes no file-path variable — defaults apply), and the RDS API
// is the source of truth.
func readAuditRDSExplicit(ctx context.Context, sourceDB *sql.DB, opts AuditReadOptions, tailLines int) (AuditReadResult, error) {
	var path string
	variant := AuditVariant("")
	var warns []string
	if sourceDB != nil {
		path, variant, warns, _ = discoverAuditLogFile(ctx, sourceDB)
	}
	if variant == "" {
		// RDS MySQL/MariaDB audit plugins and Aurora Advanced Auditing all
		// write the MariaDB server_audit dialect.
		variant = AuditVariantMariaDB
	}
	return readAuditRDS(ctx, opts, path, variant, warns, tailLines)
}

// readAuditRDS reads the audit log through the RDS file API. localPath and
// variant come from server discovery when available; an empty localPath
// falls back to the default audit/ directory.
func readAuditRDS(
	ctx context.Context,
	opts AuditReadOptions,
	localPath string,
	variant AuditVariant,
	priorWarnings []string,
	tailLines int,
) (AuditReadResult, error) {
	res := AuditReadResult{Source: AuditSourceRDS, Variant: variant, Warnings: priorWarnings}

	host := normalizeAuditHost(opts.SourceHost)
	dbID, region, ok := parseRDSHost(host)
	if !ok {
		return res, fmt.Errorf(
			"audit source %q requires an RDS endpoint host (<id>.<hash>.<region>.rds.amazonaws.com) in SourceHost, got %q",
			AuditSourceRDS, opts.SourceHost)
	}

	client, err := newRDSLogClient(ctx, region)
	if err != nil {
		return res, classifyRDSError(err, dbID, region)
	}

	files, striped, collectWarns, err := collectRDSAuditLogFiles(ctx, client, dbID, region, localPath, opts.IncludeRotated)
	res.Warnings = append(res.Warnings, collectWarns...)
	if err != nil {
		return res, err
	}
	if len(files) == 0 {
		return res, fmt.Errorf(
			"%w: no audit log files found via the RDS API for instance %q "+
				"(verify server_audit_logging is ON and the audit option group / advanced auditing is attached)",
			ErrAuditFileNotFound, dbID)
	}

	format := formatFromVariant(variant)
	res.FormatDetected = format
	if format == AuditFormatUnknown {
		return res, ErrAuditUnknownFormat
	}
	res.FilePath = files[0]
	res.FilesRead = len(files)

	filter := auditLogFilter{
		since:     opts.Since,
		until:     opts.Until,
		user:      opts.User,
		eventType: opts.EventType,
	}
	parsed, err := parseRDSAuditLogFiles(ctx, client, dbID, region, files, format, filter, opts.Offset, opts.Limit, clampTailLines(tailLines), striped)
	if parsed != nil {
		res.Events = parsed.events
		res.TotalScanned = parsed.totalScanned
		res.SkippedLines = parsed.skippedLines
		res.Warnings = append(res.Warnings, parsed.warnings...)
	}
	return res, err
}
