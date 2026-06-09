package config

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

// ScrubDSNError strips every DSN (and its password) from an error before the
// message travels somewhere a DSN must not appear — a stored job state, the
// console browser, a daemon log line. A convenience wrapper over ScrubDSNText.
func ScrubDSNError(err error, dsns ...string) string {
	return ScrubDSNText(err.Error(), dsns...)
}

// ScrubDSNText replaces every occurrence of each DSN with "<dsn>" and each
// DSN's password with "***" in msg. Empty DSNs are ignored.
func ScrubDSNText(msg string, dsns ...string) string {
	for _, dsn := range dsns {
		if dsn == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, dsn, "<dsn>")
		if cfg, err := mysql.ParseDSN(dsn); err == nil && cfg.Passwd != "" {
			msg = strings.ReplaceAll(msg, cfg.Passwd, "***")
		}
	}
	return msg
}
