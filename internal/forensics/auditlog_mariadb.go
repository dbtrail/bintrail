package forensics

import (
	"io"
	"strings"
	"time"
)

// MariaDB server_audit family CSV parser.
//
// The record layout is fixed across the family (verified against upstream
// server_audit.c, the AWS fork aws/audit-plugin-for-mysql, and the Aurora
// Advanced Auditing docs):
//
//	timestamp,serverhost,username,host,connectionid,queryid,operation,database,object,retcode
//
// For QUERY-class operations the object field is the SQL text wrapped in
// single quotes with backslash escapes (\' \\ \n \r \t \b \f). Records never
// span lines (newlines inside queries are escaped), and passwords in
// GRANT/CREATE USER statements are masked by the plugin before writing.
// Commas inside the quoted query are NOT escaped — a naive comma split is
// wrong; the object field must be scanned quote-aware.
//
// One parser covers three dialects, detected per line from the timestamp
// shape and tolerated field counts:
//
//  1. upstream MariaDB: 10 fields, timestamp "YYYYMMDD HH:MM:SS" in
//     server-LOCAL time (the plugin stamps via localtime_r());
//  2. AWS RDS MySQL fork: QUERY rows carry two trailing empty fields after
//     retcode (12 fields when naively split); CONNECT/DISCONNECT rows append
//     connection_type (11 fields);
//  3. Aurora Advanced Auditing: timestamp is epoch-MICROSECONDS (integer),
//     fields otherwise identical.

// mariadbLocalTimeNote is attached (once per parse) whenever local-time
// dialect lines were seen: the plugin writes server-local timestamps with no
// zone marker, so bintrail treats them as UTC. RDS hosts run UTC at the OS
// level; self-hosted MariaDB may not.
const mariadbLocalTimeNote = "MariaDB-family audit timestamps are in server-local time with no zone marker; " +
	"treating them as UTC (correct on RDS, verify the server timezone on self-hosted MariaDB)"

// minEpochMicrosDigits distinguishes an Aurora epoch-microseconds timestamp
// (16 digits in the current era) from other all-digit tokens: anything
// shorter than 13 digits cannot be a plausible microsecond epoch and is
// rejected rather than decoded into a nonsense date.
const minEpochMicrosDigits = 13

// parseMariaDBFile parses the file-based audit log from the MariaDB
// server_audit plugin family (upstream MariaDB, AWS RDS MySQL fork, Aurora
// Advanced Auditing). Malformed or oversized lines are skipped and counted;
// notes carries at most one local-time caveat when local-time dialect lines
// were parsed.
func parseMariaDBFile(r io.Reader, filter auditLogFilter) (events []AuditEvent, totalScanned, skipped int, notes []string, err error) {
	scanner := newAuditLineScanner(r)
	localTimeSeen := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		ev, localTime, ok := parseMariaDBLine(line)
		if !ok {
			skipped++
			continue
		}
		localTimeSeen = localTimeSeen || localTime
		totalScanned++

		if filter.afterWindow(&ev) {
			break
		}
		if !filter.matches(&ev) {
			continue
		}
		events = append(events, ev)
		if len(events) >= maxEventsPerFile {
			break
		}
	}
	if localTimeSeen {
		notes = append(notes, mariadbLocalTimeNote)
	}
	skipped = foldOversized(scanner, skipped)
	return events, totalScanned, skipped, notes, scanner.Err()
}

// parseMariaDBLine parses one audit record. localTime reports whether the
// line used the server-local timestamp dialect; ok is false for lines that
// match no known dialect (wrong field count or unrecognisable timestamp).
func parseMariaDBLine(line string) (ev AuditEvent, localTime bool, ok bool) {
	// The first 8 fields (timestamp..database) are written by the plugin
	// without quoting or escaping, so a naive split is exact for them. The
	// remainder is object[,retcode[,extras]] where a QUERY object is
	// single-quoted and may contain commas — scanned quote-aware below.
	parts := strings.SplitN(line, ",", 9)
	if len(parts) < 9 {
		return AuditEvent{}, false, false
	}

	timestamp, localTime, ok := parseMariaDBTimestamp(parts[0])
	if !ok {
		return AuditEvent{}, false, false
	}

	var sqlText, retcode string
	rest := parts[8]
	if strings.HasPrefix(rest, "'") {
		var terminated bool
		sqlText, rest, terminated = scanSingleQuoted(rest)
		if terminated {
			// rest is ",retcode" for upstream/Aurora, ",retcode,," for RDS
			// fork QUERY rows — the retcode is the first segment either
			// way; trailing empties are tolerated and ignored.
			segs := strings.SplitN(strings.TrimPrefix(rest, ","), ",", 2)
			retcode = segs[0]
		}
		// Unterminated quote: a truncated or corrupted record (the plugin
		// itself always closes the quote, even under
		// SERVER_AUDIT_QUERY_LOG_LIMIT truncation, but partially written or
		// copied files may not). Keep the query text read so far — the
		// event is still forensically valuable — with no retcode.
	} else {
		// Non-quoted object: CONNECT/DISCONNECT/FAILED_CONNECT (empty
		// object) and table operations (object = table name). The RDS fork
		// appends connection_type after retcode on connection events (11
		// naive fields); extras are tolerated and ignored.
		segs := strings.Split(rest, ",")
		sqlText = segs[0]
		if len(segs) > 1 {
			retcode = segs[1]
		}
	}

	ev = AuditEvent{
		Timestamp:    timestamp,
		User:         parts[2],
		Host:         parts[3],
		ConnectionID: parseInt64(parts[4]),
		EventType:    parts[6],
		DB:           parts[7],
		SQLText:      sqlText,
		Status:       int(parseInt64(retcode)),
	}
	return ev, localTime, true
}

// parseMariaDBTimestamp validates and normalises the timestamp field,
// detecting the dialect from its shape:
//
//   - all-digits (>= minEpochMicrosDigits) → Aurora epoch-microseconds,
//     normalised to RFC 3339 UTC so downstream filters and consumers never
//     see a raw integer;
//   - "YYYYMMDD HH:MM:SS" → upstream MariaDB / RDS fork server-local time,
//     kept verbatim (parseFlexTimestamp reads this layout as UTC).
func parseMariaDBTimestamp(field string) (formatted string, localTime bool, ok bool) {
	field = strings.TrimSpace(field)
	if isAllDigits(field) {
		if len(field) < minEpochMicrosDigits {
			return "", false, false
		}
		micros := parseInt64(field)
		if micros == 0 {
			return "", false, false
		}
		return time.UnixMicro(micros).UTC().Format(time.RFC3339Nano), false, true
	}
	if _, err := time.Parse("20060102 15:04:05", field); err != nil {
		return "", false, false
	}
	return field, true, true
}

// scanSingleQuoted parses a single-quoted, backslash-escaped audit object
// field; s must start with a single quote. It returns the unescaped content,
// the remainder of the line after the closing quote, and whether the closing
// quote was found. Escapes are the exact set server_audit.c writes: \' \\
// \n \r \t \b \f. Any other backslash pair is preserved verbatim for
// forensic fidelity.
func scanSingleQuoted(s string) (content, rest string, terminated bool) {
	var b strings.Builder
	i := 1 // skip the opening quote
	for i < len(s) {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case '\'', '\\':
				b.WriteByte(s[i+1])
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i+1])
			}
			i += 2
			continue
		}
		if ch == '\'' {
			return b.String(), s[i+1:], true
		}
		b.WriteByte(ch)
		i++
	}
	return b.String(), "", false
}

// isAllDigits reports whether s is non-empty and consists solely of ASCII
// digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
