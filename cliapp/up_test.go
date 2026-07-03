package cliapp

import (
	"strings"
	"testing"
)

// This file mutates up*/strm* package globals via save-and-restore. DO NOT
// add t.Parallel() to any test here or to sibling tests that read strm*
// globals — concurrent runs would cross-write each other's state. The whole
// package effectively serializes on these globals already (runStream/runUp
// read them at start), so parallelism is not a real benefit anyway.

// TestPopulateStreamFlags asserts the up* → strm* fan-out so a flag added to
// up but not wired into runStream fails CI rather than silently dropping the
// value.
func TestPopulateStreamFlags(t *testing.T) {
	// ── Save originals ─────────────────────────────────────────────────────
	orig := struct {
		// up* side
		uSrc, uIdx, uSch, uTbl, uMet, uFmt string
		uSID                               uint32
		uBatch, uChk                       int
		// strm* side
		sSrc, sIdx, sFile, sGTID, sSch, sTbl, sMet, sFmt, sSSL, sCA, sCert, sKey string
		sSID                                                                     uint32
		sPos                                                                     uint32
		sBatch, sChk, sGap                                                       int
		sReset, sNoGap                                                           bool
	}{
		uSrc: upSourceDSN, uIdx: upIndexDSN, uSID: upServerID,
		uSch: upSchemas, uTbl: upTables, uBatch: upBatchSize,
		uChk: upCheckpoint, uMet: upMetricsAddr, uFmt: upFormat,
		sSrc: strmSourceDSN, sIdx: strmIndexDSN, sFile: strmStartFile,
		sGTID: strmStartGTID, sSch: strmSchemas, sTbl: strmTables,
		sMet: strmMetricsAddr, sFmt: strmFormat, sSSL: strmSSLMode,
		sCA: strmSSLCA, sCert: strmSSLCert, sKey: strmSSLKey,
		sSID: strmServerID, sPos: strmStartPos,
		sBatch: strmBatchSize, sChk: strmCheckpoint, sGap: strmGapTimeout,
		sReset: strmReset, sNoGap: strmNoGapFill,
	}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upServerID = orig.uSrc, orig.uIdx, orig.uSID
		upSchemas, upTables, upBatchSize = orig.uSch, orig.uTbl, orig.uBatch
		upCheckpoint, upMetricsAddr, upFormat = orig.uChk, orig.uMet, orig.uFmt
		strmSourceDSN, strmIndexDSN, strmStartFile = orig.sSrc, orig.sIdx, orig.sFile
		strmStartGTID, strmSchemas, strmTables = orig.sGTID, orig.sSch, orig.sTbl
		strmMetricsAddr, strmFormat, strmSSLMode = orig.sMet, orig.sFmt, orig.sSSL
		strmSSLCA, strmSSLCert, strmSSLKey = orig.sCA, orig.sCert, orig.sKey
		strmServerID, strmStartPos = orig.sSID, orig.sPos
		strmBatchSize, strmCheckpoint, strmGapTimeout = orig.sBatch, orig.sChk, orig.sGap
		strmReset, strmNoGapFill = orig.sReset, orig.sNoGap
	})

	// ── Set up* to known values ─────────────────────────────────────────────
	upSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	upIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	upSchemas = "mydb,otherdb"
	upTables = "mydb.orders"
	upBatchSize = 2500
	upCheckpoint = 15
	upMetricsAddr = ":9091"
	upFormat = "json"

	const passedServerID uint32 = 4242424242

	// ── Act ─────────────────────────────────────────────────────────────────
	populateStreamFlags(passedServerID)

	// ── Assert mapped fields ────────────────────────────────────────────────
	assertStr(t, "strmIndexDSN", strmIndexDSN, upIndexDSN)
	assertStr(t, "strmSourceDSN", strmSourceDSN, upSourceDSN)
	assertStr(t, "strmSchemas", strmSchemas, upSchemas)
	assertStr(t, "strmTables", strmTables, upTables)
	assertStr(t, "strmMetricsAddr", strmMetricsAddr, upMetricsAddr)
	assertStr(t, "strmFormat", strmFormat, upFormat)
	if strmServerID != passedServerID {
		t.Errorf("strmServerID = %d, want %d", strmServerID, passedServerID)
	}
	if strmBatchSize != upBatchSize {
		t.Errorf("strmBatchSize = %d, want %d", strmBatchSize, upBatchSize)
	}
	if strmCheckpoint != upCheckpoint {
		t.Errorf("strmCheckpoint = %d, want %d", strmCheckpoint, upCheckpoint)
	}

	// ── Assert defaults set by populateStreamFlags ──────────────────────────
	// These are not user-configurable from `up` — populateStreamFlags pins
	// them. Asserting on them locks in the intentional defaults so a future
	// "let me delete this unused field" refactor can't silently change behavior.
	assertStr(t, "strmStartFile", strmStartFile, "")
	assertStr(t, "strmStartGTID", strmStartGTID, "")
	assertStr(t, "strmSSLMode", strmSSLMode, "preferred")
	assertStr(t, "strmSSLCA", strmSSLCA, "")
	assertStr(t, "strmSSLCert", strmSSLCert, "")
	assertStr(t, "strmSSLKey", strmSSLKey, "")
	if strmStartPos != 4 {
		t.Errorf("strmStartPos = %d, want 4 (binlog magic-number header end)", strmStartPos)
	}
	if strmGapTimeout != 30 {
		t.Errorf("strmGapTimeout = %d, want 30", strmGapTimeout)
	}
	if strmReset || strmNoGapFill {
		t.Errorf("strmReset=%v strmNoGapFill=%v, want both false", strmReset, strmNoGapFill)
	}
}

func assertStr(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

// TestRunUpSourceDSNValidation: --source-dsn is required — the gate fires in
// runUp (after the rotation-settings parse) rather than via MarkFlagRequired.
// The source-less daemon moved to `bintrail-console watch`.
func TestRunUpSourceDSNValidation(t *testing.T) {
	origSrc, origIdx, origFmt := upSourceDSN, upIndexDSN, upFormat
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upFormat = origSrc, origIdx, origFmt
	})
	upFormat = "text"

	upSourceDSN = ""
	upIndexDSN = "u:p@tcp(127.0.0.1:3306)/idx"
	err := runUp(upCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--source-dsn is required") {
		t.Fatalf("source-less up: err = %v, want the --source-dsn requirement", err)
	}
}
