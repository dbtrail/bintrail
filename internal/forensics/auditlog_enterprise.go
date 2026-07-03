package forensics

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// MySQL Enterprise + Percona JSON parser, and the legacy MySQL Enterprise
// XML parser. Both vendors' JSON logs are line-delimited self-contained
// objects; the normaliser handles the vendor-specific field names.

// parseAuditJSON parses JSON-format audit logs used by both MySQL Enterprise
// (8.0+) and the Percona Audit Log Plugin. Each line is a self-contained
// JSON object (optionally wrapped in a JSON array with trailing commas).
//
// The filter is applied inline so the per-file cap (maxEventsPerFile) bounds
// matched events, not scanned lines. Time-ordered formats get early-exit
// when the timestamp passes filter.until.
func parseAuditJSON(r io.Reader, filter auditLogFilter) ([]AuditEvent, int, int, error) {
	var events []AuditEvent
	totalScanned, skipped := 0, 0
	scanner := newAuditLineScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line == "[" || line == "]" {
			continue
		}
		// Strip trailing comma (JSON array format).
		line = strings.TrimRight(line, ",")

		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			skipped++
			continue
		}

		ev := normalizeJSONEvent(raw)
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

// normalizeJSONEvent converts a raw JSON audit log entry (MySQL Enterprise
// or Percona) into the common event structure.
func normalizeJSONEvent(raw map[string]any) AuditEvent {
	ev := AuditEvent{}

	// Timestamp — both MySQL Enterprise and Percona use "timestamp".
	ev.Timestamp = jsonStr(raw, "timestamp")

	// Event class/type — MySQL Enterprise uses "class" + "event", Percona
	// uses "name" or "class".
	eventType := jsonStr(raw, "class")
	if sub := jsonStr(raw, "event"); sub != "" {
		eventType = eventType + "/" + sub
	}
	if eventType == "" || eventType == "/" {
		eventType = jsonStr(raw, "name")
	}
	ev.EventType = eventType

	// Connection/login info — nested under different keys per vendor.
	if login, ok := raw["login"].(map[string]any); ok {
		ev.User = jsonStr(login, "user")
		ev.Host = jsonStr(login, "host")
	}
	if conn, ok := raw["connection_data"].(map[string]any); ok {
		if ev.Host == "" {
			ev.Host = jsonStr(conn, "host")
		}
		ev.DB = jsonStr(conn, "db")
	}

	// General/query info.
	if general, ok := raw["general_data"].(map[string]any); ok {
		ev.SQLText = jsonStr(general, "query")
		ev.User = coalesce(ev.User, jsonStr(general, "user"))
		ev.Host = coalesce(ev.Host, jsonStr(general, "ip"))
	}

	// Direct fields (Percona-style flat JSON).
	ev.User = coalesce(ev.User, jsonStr(raw, "user"))
	ev.Host = coalesce(ev.Host, jsonStr(raw, "host"), jsonStr(raw, "ip"))
	ev.SQLText = coalesce(ev.SQLText, jsonStr(raw, "sqltext"), jsonStr(raw, "query"))
	ev.DB = coalesce(ev.DB, jsonStr(raw, "db"))
	ev.ConnectionID = jsonInt64(raw, "connection_id")
	ev.Status = int(jsonInt64(raw, "status"))

	return ev
}

// parseAuditXML parses the legacy XML audit log format used by the MySQL
// Enterprise Audit Plugin. The file contains <AUDIT_RECORD> elements.
func parseAuditXML(r io.Reader, filter auditLogFilter) ([]AuditEvent, int, int, error) {
	var events []AuditEvent
	totalScanned, skipped := 0, 0
	decoder := xml.NewDecoder(r)

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Return what we have so far on parse error.
			return events, totalScanned, skipped, err
		}

		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "AUDIT_RECORD" {
			continue
		}

		var rec xmlAuditRecord
		if err := decoder.DecodeElement(&rec, &se); err != nil {
			skipped++
			continue
		}

		ev := AuditEvent{
			Timestamp:    rec.Timestamp,
			User:         rec.User,
			Host:         rec.Host,
			EventType:    rec.Name,
			SQLText:      rec.SQLText,
			Status:       rec.Status,
			ConnectionID: rec.ConnectionID,
			DB:           rec.DB,
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
	return events, totalScanned, skipped, nil
}

type xmlAuditRecord struct {
	Timestamp    string `xml:"TIMESTAMP"`
	User         string `xml:"USER"`
	Host         string `xml:"HOST"`
	Name         string `xml:"NAME"`
	SQLText      string `xml:"SQLTEXT"`
	Status       int    `xml:"STATUS"`
	ConnectionID int64  `xml:"CONNECTION_ID"`
	DB           string `xml:"DB"`
}

// jsonStr extracts a string value from a map.
func jsonStr(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// jsonInt64 extracts an integer value from a map (JSON numbers are float64).
func jsonInt64(m map[string]any, key string) int64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}
