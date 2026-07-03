// Package forensics provides audit-log and connection forensics primitives
// for MySQL-family servers: audit plugin discovery via SHOW GLOBAL VARIABLES,
// on-disk audit log parsing (MariaDB/RDS/Aurora CSV dialects, Percona CSV,
// Percona/MySQL Enterprise JSON, MySQL Enterprise XML), and normalisation of
// vendor events into a common shape.
//
// Remote log sources (RDS/CloudWatch APIs) are intentionally out of scope
// here — this package only reads files reachable on the local filesystem.
package forensics

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AuditEvent is the normalised representation of a single audit log entry,
// regardless of the originating vendor or format. JSON field names match the
// SaaS agent contract.
type AuditEvent struct {
	Timestamp    string `json:"timestamp"`
	User         string `json:"user,omitempty"`
	Host         string `json:"host,omitempty"`
	EventType    string `json:"event_type,omitempty"`
	SQLText      string `json:"sql_text,omitempty"`
	Status       int    `json:"status"`
	ConnectionID int64  `json:"connection_id,omitempty"`
	DB           string `json:"db,omitempty"`
}

// AuditVariant identifies the audit plugin family that produced a log.
type AuditVariant string

// Known audit plugin variants.
const (
	AuditVariantMySQLEnterprise AuditVariant = "mysql_enterprise"
	AuditVariantPercona         AuditVariant = "percona"
	AuditVariantMariaDB         AuditVariant = "mariadb"
)

// AuditFormat identifies the on-disk audit log file format.
type AuditFormat string

// Supported audit log formats.
const (
	AuditFormatJSON AuditFormat = "json"
	AuditFormatXML  AuditFormat = "xml"
	// AuditFormatCSV is the Percona audit_log CSV format (double-quoted
	// fields).
	AuditFormatCSV AuditFormat = "csv"
	// AuditFormatMariaDB is the MariaDB server_audit family CSV format
	// (single-quoted query field), covering upstream MariaDB, the AWS RDS
	// MySQL fork, and Aurora Advanced Auditing dialects.
	AuditFormatMariaDB AuditFormat = "mariadb"
	AuditFormatUnknown AuditFormat = "unknown"
)

// AuditReadOptions controls ReadAuditLog discovery, filtering, and paging.
type AuditReadOptions struct {
	// Since / Until bound events by timestamp. Zero values disable the bound.
	// Until is exclusive.
	Since time.Time
	Until time.Time
	// User and EventType filter case-insensitively on exact match.
	User      string
	EventType string
	// Limit caps returned events. Values <= 0 or > 10000 fall back to 500.
	Limit int
	// Offset skips the first N matched events (applied after filtering,
	// across files).
	Offset int
	// IncludeRotated also parses rotated variants of the audit log file
	// (audit.log.1, audit.log-20240101, ...), newest first, capped at
	// maxRotatedFiles.
	IncludeRotated bool
	// TailLines controls tail-mode reading:
	//   > 0: read only approximately the last N lines of each file
	//        (estimated at 256 bytes/line, seeking near the end);
	//     0: auto — defaults to 10000 when Since is set, full scan otherwise;
	//   < 0: force a full scan even when Since is set.
	TailLines int
}

// AuditReadResult is the outcome of ReadAuditLog. JSON field names match the
// SaaS agent response contract.
type AuditReadResult struct {
	Events         []AuditEvent `json:"events"`
	TotalScanned   int          `json:"total_scanned"`
	SkippedLines   int          `json:"skipped_lines,omitempty"`
	FormatDetected AuditFormat  `json:"format_detected"`
	Variant        AuditVariant `json:"variant,omitempty"`
	FilePath       string       `json:"file_path"`
	FilesRead      int          `json:"files_read"`
	Warnings       []string     `json:"warnings,omitempty"`
}

// Sentinel errors for the discovery pipeline. Callers can errors.Is on these
// to map them to their surface's error taxonomy (CLI exit codes, MCP errors,
// agent responses).
var (
	// ErrAuditNotConfigured means neither audit_log_file (MySQL
	// Enterprise / Percona) nor server_audit_file_path (MariaDB family) is
	// set on the server: no audit plugin is configured.
	ErrAuditNotConfigured = errors.New("no audit log file configured on this server")
	// ErrAuditFileNotFound means the server reports an audit log path but
	// no such file exists on the local filesystem (e.g. bintrail runs on a
	// different host than mysqld).
	ErrAuditFileNotFound = errors.New("audit log file not found on disk")
	// ErrAuditUnknownFormat means the audit log file content matched none
	// of the supported formats.
	ErrAuditUnknownFormat = errors.New("could not detect audit log format; supported formats are JSON, XML, and CSV")
)

const (
	// maxEventsPerFile caps how many matched events are accumulated from a
	// single file to prevent unbounded memory growth on multi-GB audit
	// logs. The cap applies to events *after* filtering, so a time-filtered
	// request can still scan to EOF on large files — only the accumulated
	// output is bounded.
	maxEventsPerFile = 100_000

	// maxRotatedFiles caps how many rotated log files are scanned.
	maxRotatedFiles = 20

	// defaultTailLines is the number of lines fetched from the end of the
	// log file when the caller specifies a Since filter but no explicit
	// TailLines. 10,000 lines ≈ 1 MB for typical MariaDB audit logs —
	// enough to cover several minutes of a very busy server without
	// scanning gigabytes of history.
	defaultTailLines = 10_000

	// tailBytesPerLine is the conservative bytes-per-line estimate used to
	// translate TailLines into a byte offset from EOF.
	tailBytesPerLine = 256

	auditDefaultLimit = 500
	auditMaxLimit     = 10_000
)

// ReadAuditLog discovers the audit log configured on sourceDB, parses the
// on-disk file(s), and returns events matching opts. The flow is:
//
//  1. SHOW GLOBAL VARIABLES LIKE 'audit_log_file' (MySQL Enterprise /
//     Percona), then 'server_audit_file_path' (MariaDB family);
//  2. variant disambiguation via information_schema.PLUGINS;
//  3. relative paths resolved against the server datadir, with path
//     traversal hardening;
//  4. rotated-file collection (opt-in, capped), tail-mode seeking, and
//     format-specific parsing with since/until/user/event_type filters and
//     offset/limit paging.
//
// Parse errors inside a file are non-fatal: they surface as Warnings on the
// result alongside the events parsed so far. On a non-nil error the returned
// result still carries any warnings accumulated before the failure.
func ReadAuditLog(ctx context.Context, sourceDB *sql.DB, opts AuditReadOptions) (AuditReadResult, error) {
	if opts.Limit <= 0 || opts.Limit > auditMaxLimit {
		opts.Limit = auditDefaultLimit
	}
	tailLines := resolveTailLines(opts.TailLines, opts.Since)

	var res AuditReadResult

	filePath, variant, warns, discoverErr := discoverAuditLogFile(ctx, sourceDB)
	res.Warnings = append(res.Warnings, warns...)
	if filePath == "" {
		if discoverErr != nil {
			return res, fmt.Errorf("failed to query audit log configuration: %w", discoverErr)
		}
		return res, ErrAuditNotConfigured
	}
	res.Variant = variant

	// Defence-in-depth: reject traversal components in the raw variable
	// value BEFORE filepath.Join/Clean resolve them, so a crafted MySQL
	// variable like "/var/lib/mysql/../../etc/passwd" (or a relative
	// "logs/../../etc/passwd") is caught rather than cleaned into
	// "/etc/passwd".
	if strings.Contains(filePath, "..") {
		return res, fmt.Errorf("audit log path contains path traversal: %q", filePath)
	}

	// Resolve relative paths against the MySQL data directory.
	if !filepath.IsAbs(filePath) {
		dataDir, dirWarns := discoverDataDir(ctx, sourceDB)
		res.Warnings = append(res.Warnings, dirWarns...)
		if strings.Contains(dataDir, "..") {
			return res, fmt.Errorf("datadir contains path traversal: %q", dataDir)
		}
		if dataDir != "" {
			filePath = filepath.Join(dataDir, filePath)
		}
	}
	filePath = filepath.Clean(filePath)
	if !filepath.IsAbs(filePath) {
		return res, fmt.Errorf("audit log path must be absolute, got %q", filePath)
	}
	res.FilePath = filePath

	// Collect files to parse (current + optionally rotated).
	files, collectWarns := collectAuditLogFiles(filePath, opts.IncludeRotated)
	res.Warnings = append(res.Warnings, collectWarns...)
	if len(files) == 0 {
		return res, fmt.Errorf("%w: %s", ErrAuditFileNotFound, filePath)
	}

	format := detectAuditLogFormat(files[0], variant)
	res.FormatDetected = format
	if format == AuditFormatUnknown {
		return res, ErrAuditUnknownFormat
	}

	filter := auditLogFilter{
		since:     opts.Since,
		until:     opts.Until,
		user:      opts.User,
		eventType: opts.EventType,
	}
	// Translate tail lines into approximate tail bytes; the parser scans
	// from the seek point forward and filter.matches drops anything still
	// outside the requested time window.
	var tailBytes int64
	if tailLines > 0 {
		tailBytes = int64(tailLines) * tailBytesPerLine
	}

	parsed, err := parseAuditLogFiles(files, format, filter, opts.Offset, opts.Limit, tailBytes)
	if err != nil {
		return res, err
	}
	res.Events = parsed.events
	res.TotalScanned = parsed.totalScanned
	res.SkippedLines = parsed.skippedLines
	res.Warnings = append(res.Warnings, parsed.warnings...)
	res.FilesRead = len(files)
	return res, nil
}

// resolveTailLines applies the TailLines auto-default: callers that ask
// "what happened since time T" are almost always interested in recent
// history, so a Since filter without an explicit TailLines defaults to
// tailing rather than scanning the whole file. Negative values force a full
// scan.
func resolveTailLines(tailLines int, since time.Time) int {
	switch {
	case tailLines < 0:
		return 0
	case tailLines == 0 && !since.IsZero():
		return defaultTailLines
	default:
		return tailLines
	}
}

// ---------------------------------------------------------------------------
// Audit log file discovery
// ---------------------------------------------------------------------------

// discoverAuditLogFile queries MySQL for the configured audit log file path
// and the plugin variant. Returns an empty path when no audit log is
// configured; err is non-nil only when the lookups failed for a reason other
// than the variable not existing (e.g. connectivity loss), so callers can
// distinguish "no plugin" from "could not ask".
func discoverAuditLogFile(ctx context.Context, db *sql.DB) (path string, variant AuditVariant, warns []string, err error) {
	// Try MySQL Enterprise / Percona first.
	var varName, varValue string
	scanErr := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'audit_log_file'").Scan(&varName, &varValue)
	if scanErr == nil && varValue != "" {
		variant, vwarns := detectVariantFromPlugin(ctx, db)
		return varValue, variant, vwarns, nil
	}
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		warns = append(warns, fmt.Sprintf("failed to query audit_log_file variable: %v", scanErr))
		err = scanErr
	}

	// Try MariaDB server_audit.
	scanErr = db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'server_audit_file_path'").Scan(&varName, &varValue)
	if scanErr == nil && varValue != "" {
		return varValue, AuditVariantMariaDB, warns, nil
	}
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		warns = append(warns, fmt.Sprintf("failed to query server_audit_file_path variable: %v", scanErr))
		err = scanErr
	}

	return "", "", warns, err
}

// detectVariantFromPlugin checks the active audit plugin to distinguish
// MySQL Enterprise from Percona (both expose audit_log_file).
func detectVariantFromPlugin(ctx context.Context, db *sql.DB) (AuditVariant, []string) {
	rows, err := db.QueryContext(ctx,
		"SELECT PLUGIN_NAME, PLUGIN_DESCRIPTION FROM information_schema.PLUGINS "+
			"WHERE UPPER(PLUGIN_NAME) LIKE '%AUDIT%' AND PLUGIN_STATUS = 'ACTIVE'",
	)
	if err != nil {
		return AuditVariantMySQLEnterprise, []string{
			fmt.Sprintf("could not query audit plugins for variant detection: %v", err),
		}
	}
	defer rows.Close()

	var warns []string
	for rows.Next() {
		var name, desc string
		if err := rows.Scan(&name, &desc); err != nil {
			warns = append(warns, fmt.Sprintf("skipping plugin row during variant detection: %v", err))
			continue
		}
		upper := strings.ToUpper(name) + " " + strings.ToUpper(desc)
		if strings.Contains(upper, "PERCONA") {
			return AuditVariantPercona, warns
		}
		if strings.Contains(upper, "SERVER_AUDIT") || strings.Contains(upper, "MARIADB") {
			return AuditVariantMariaDB, warns
		}
	}
	return AuditVariantMySQLEnterprise, warns
}

// discoverDataDir returns the MySQL data directory for resolving relative
// audit log paths.
func discoverDataDir(ctx context.Context, db *sql.DB) (string, []string) {
	var name, value string
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'datadir'").Scan(&name, &value); err != nil {
		return "", []string{fmt.Sprintf("could not query datadir: %v", err)}
	}
	return value, nil
}

// collectAuditLogFiles returns the primary audit log file and, when
// includeRotated is true, any rotated variants (*.1, *.2, audit.log-20240101,
// ...) sorted newest-first by mtime, capped at maxRotatedFiles.
func collectAuditLogFiles(primary string, includeRotated bool) ([]string, []string) {
	var files []string
	if _, err := os.Stat(primary); err == nil {
		files = append(files, primary)
	}
	if !includeRotated {
		return files, nil
	}

	dir := filepath.Dir(primary)
	base := filepath.Base(primary)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return files, []string{fmt.Sprintf("could not list directory %s for rotated files: %v", dir, err)}
	}
	rotated := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == base {
			continue // already added
		}
		// Match patterns like "audit.log.1" or "audit.log-20240101".
		if strings.HasPrefix(name, base+".") || strings.HasPrefix(name, base+"-") {
			files = append(files, filepath.Join(dir, name))
			rotated++
			if rotated >= maxRotatedFiles {
				break
			}
		}
	}

	// Sort: primary first, then rotated newest-first (higher suffix = older
	// in most rotation schemes, but we sort by mtime descending for
	// accuracy).
	if len(files) > 1 {
		sort.Slice(files[1:], func(i, j int) bool {
			fi, _ := os.Stat(files[1+i])
			fj, _ := os.Stat(files[1+j])
			if fi == nil || fj == nil {
				return files[1+i] > files[1+j]
			}
			return fi.ModTime().After(fj.ModTime())
		})
	}
	return files, nil
}

// ---------------------------------------------------------------------------
// Format detection
// ---------------------------------------------------------------------------

// detectAuditLogFormat peeks at the first non-empty bytes of a file to
// determine the format. The variant hint from MySQL plugin detection helps
// disambiguate when the content alone is not decisive.
func detectAuditLogFormat(path string, variant AuditVariant) AuditFormat {
	f, err := os.Open(path)
	if err != nil {
		return AuditFormatUnknown
	}
	defer f.Close()

	buf := make([]byte, 512)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return AuditFormatUnknown
	}
	sample := strings.TrimSpace(string(buf[:n]))

	// Check extension first as a strong hint.
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return AuditFormatJSON
	case ".xml":
		return AuditFormatXML
	case ".csv":
		if variant == AuditVariantMariaDB {
			return AuditFormatMariaDB
		}
		return AuditFormatCSV
	}

	// Content-based detection.
	if len(sample) == 0 {
		return AuditFormatUnknown
	}
	switch sample[0] {
	case '{', '[':
		return AuditFormatJSON
	case '<':
		return AuditFormatXML
	}

	// MariaDB-family plugins write comma-separated lines starting with a
	// timestamp like "20240101 12:00:00" (or epoch-microseconds on Aurora).
	if variant == AuditVariantMariaDB {
		return AuditFormatMariaDB
	}

	// Percona CSV: lines starting with a quoted timestamp.
	if sample[0] == '"' {
		return AuditFormatCSV
	}

	return AuditFormatUnknown
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

type auditLogFilter struct {
	since     time.Time
	until     time.Time
	user      string
	eventType string
}

func (f *auditLogFilter) matches(ev *AuditEvent) bool {
	// Parse timestamp once for both bounds checks. If time bounds are set
	// but the timestamp is unparseable, exclude the event rather than
	// silently including it.
	if !f.since.IsZero() || !f.until.IsZero() {
		ts, err := parseFlexTimestamp(ev.Timestamp)
		if err != nil {
			return false
		}
		if !f.since.IsZero() && ts.Before(f.since) {
			return false
		}
		if !f.until.IsZero() && !ts.Before(f.until) {
			return false
		}
	}
	if f.user != "" && !strings.EqualFold(ev.User, f.user) {
		return false
	}
	if f.eventType != "" && !strings.EqualFold(ev.EventType, f.eventType) {
		return false
	}
	return true
}

// afterWindow returns true if the event's timestamp is at or after the
// filter's upper bound. Audit logs are append-only time-ordered, so once we
// observe a timestamp past `until` we know every subsequent event is also
// out of range and the parser can stop. Returns false when `until` is unset
// or the timestamp can't be parsed (fall through to normal matching — a lone
// unparseable timestamp should not terminate a scan).
func (f *auditLogFilter) afterWindow(ev *AuditEvent) bool {
	if f.until.IsZero() {
		return false
	}
	ts, err := parseFlexTimestamp(ev.Timestamp)
	if err != nil {
		return false
	}
	return !ts.Before(f.until)
}

// parseFlexTimestamp tries several common timestamp layouts.
func parseFlexTimestamp(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"20060102 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format: %s", s)
}

// ---------------------------------------------------------------------------
// Parsing orchestrator
// ---------------------------------------------------------------------------

// parseResult holds the output of parseAuditLogFiles.
type parseResult struct {
	events       []AuditEvent
	totalScanned int
	skippedLines int
	warnings     []string
}

// parseAuditLogFiles reads events from the given files, applies filtering
// and pagination (offset + limit), and returns the matching events. The
// filter is pushed into the parsers so per-file memory (capped at
// maxEventsPerFile) applies to matched events only — large files with a
// tight time window scan to EOF without truncation.
//
// When tailBytes > 0 and a file is larger than tailBytes, reading starts
// near the end (seek to size-tailBytes, discard the first partial line).
// Used for time-filtered queries so we don't scan gigabytes of history when
// the caller only wants the last few minutes.
//
// Per-file parse errors are non-fatal: they are appended to the result's
// warnings and parsing continues with the events collected so far.
func parseAuditLogFiles(
	files []string, format AuditFormat,
	filter auditLogFilter, offset, limit int,
	tailBytes int64,
) (*parseResult, error) {
	res := &parseResult{}
	matched := 0

	for idx, path := range files {
		if len(res.events) >= limit {
			break
		}

		f, err := os.Open(path)
		if err != nil {
			res.warnings = append(res.warnings, fmt.Sprintf("could not open %s: %v", filepath.Base(path), err))
			continue
		}

		reader, tailSeeked, seekErr := tailReader(f, tailBytes)
		if seekErr != nil {
			res.warnings = append(res.warnings, fmt.Sprintf("tail seek failed for %s, reading from start", filepath.Base(path)))
			reader = f
			tailSeeked = false
		}

		var fileEvents []AuditEvent
		var totalScanned, skipped int
		var parseErr error

		switch format {
		case AuditFormatJSON:
			fileEvents, totalScanned, skipped, parseErr = parseAuditJSON(reader, filter)
		case AuditFormatXML:
			fileEvents, totalScanned, skipped, parseErr = parseAuditXML(reader, filter)
		case AuditFormatCSV:
			fileEvents, totalScanned, skipped, parseErr = parsePerconaCSV(reader, filter)
		case AuditFormatMariaDB:
			var notes []string
			fileEvents, totalScanned, skipped, notes, parseErr = parseMariaDBFile(reader, filter)
			for _, note := range notes {
				if !slices.Contains(res.warnings, note) {
					res.warnings = append(res.warnings, note)
				}
			}
		default:
			f.Close()
			return nil, fmt.Errorf("unsupported audit log format: %s", format)
		}
		f.Close()

		res.totalScanned += totalScanned
		res.skippedLines += skipped
		if parseErr != nil {
			res.warnings = append(res.warnings, fmt.Sprintf("parse error in %s: %v", filepath.Base(path), parseErr))
		}

		// A tail-seek that found no in-window events suggests the requested
		// window is older than the tail covers. Guards:
		//   - idx == 0: only for the primary file (rotated files are
		//     legitimately older than the tail window).
		//   - tailSeeked: skip when the file fit in tailBytes and was read
		//     in full — no amount of larger tail_lines would help, so the
		//     remediation message would mislead.
		//   - totalScanned|skipped > 0: skip legitimately empty files.
		if idx == 0 && tailSeeked && !filter.since.IsZero() && len(fileEvents) == 0 && (totalScanned > 0 || skipped > 0) {
			res.warnings = append(res.warnings,
				fmt.Sprintf("tail seek of %s returned no events in the requested time window; retry with a larger tail_lines, tail_lines=-1 for a full scan, or include_rotated=true",
					filepath.Base(path)))
		}

		for i := range fileEvents {
			matched++
			if matched <= offset {
				continue
			}
			res.events = append(res.events, fileEvents[i])
			if len(res.events) >= limit {
				break
			}
		}
	}

	return res, nil
}

// tailReader returns an io.Reader positioned `tailBytes` from the end of the
// file, with the first (partial) line discarded. The second return value
// indicates whether a tail seek actually occurred — callers use this to
// decide whether a "tail miss" warning is appropriate (a file smaller than
// tailBytes was read in full, so there's nothing to enlarge). A non-nil
// error means the seek failed; callers should fall back to reading from the
// start.
func tailReader(f *os.File, tailBytes int64) (io.Reader, bool, error) {
	if tailBytes <= 0 {
		return f, false, nil
	}
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if info.Size() <= tailBytes {
		return f, false, nil
	}
	if _, err := f.Seek(info.Size()-tailBytes, io.SeekStart); err != nil {
		return nil, false, err
	}
	// Discard up to the next newline so we don't emit a half line.
	br := bufio.NewReader(f)
	if _, err := br.ReadBytes('\n'); err != nil && err != io.EOF {
		return nil, false, err
	}
	return br, true, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// parseInt64 parses a string to int64, returning 0 on failure.
func parseInt64(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// coalesce returns the first non-empty string.
func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
