package forensics

import (
	"io"
	"strings"
)

// Percona audit_log CSV parser. Unlike the MariaDB family, Percona CSV wraps
// every field in DOUBLE quotes with doubled-quote escaping (RFC-4180-style),
// so it gets its own splitter.

// parsePerconaCSV parses the CSV-format audit log produced by the Percona
// Audit Log Plugin. The format uses quoted fields separated by commas.
// Columns: "timestamp","user","host","connection_id","query_id","operation",
// "database","object","retcode".
func parsePerconaCSV(r io.Reader, filter auditLogFilter) ([]AuditEvent, int, int, error) {
	var events []AuditEvent
	totalScanned, skipped := 0, 0
	scanner := newAuditLineScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := splitCSVLine(line)
		if len(fields) < 6 {
			skipped++
			continue
		}

		ev := AuditEvent{
			Timestamp: unquote(fields[0]),
			User:      unquote(fields[1]),
			Host:      unquote(fields[2]),
			EventType: unquote(fields[5]),
		}
		if len(fields) > 3 {
			ev.ConnectionID = parseInt64(unquote(fields[3]))
		}
		if len(fields) > 6 {
			ev.DB = unquote(fields[6])
		}
		if len(fields) > 7 {
			ev.SQLText = unquote(fields[7])
		}
		if len(fields) > 8 {
			ev.Status = int(parseInt64(unquote(fields[8])))
		}

		if ev.Timestamp == "" {
			continue
		}
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
	skipped = foldOversized(scanner, skipped)
	return events, totalScanned, skipped, scanner.Err()
}

// unquote strips surrounding double-quotes from a CSV field and unescapes
// doubled quotes.
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return s
}

// splitCSVLine splits a line by commas, respecting double-quoted fields.
// Quotes are preserved on the returned fields (unquote strips them).
func splitCSVLine(line string) []string {
	var fields []string
	var field strings.Builder
	inQuote := false

	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch {
		case ch == '"' && !inQuote:
			inQuote = true
			field.WriteByte(ch)
		case ch == '"' && inQuote:
			field.WriteByte(ch)
			// Check for escaped quote ("").
			if i+1 < len(line) && line[i+1] == '"' {
				field.WriteByte('"')
				i++
			} else {
				inQuote = false
			}
		case ch == ',' && !inQuote:
			fields = append(fields, field.String())
			field.Reset()
		default:
			field.WriteByte(ch)
		}
	}
	fields = append(fields, field.String())
	return fields
}
