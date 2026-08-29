package icebergexport

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
)

func TestCursor_roundTrip(t *testing.T) {
	c := cursor{File: "binlog.000042", Pos: 1234, At: time.Date(2026, 8, 28, 12, 0, 0, 500, time.UTC)}
	got, err := readCursor(c.properties())
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != c {
		t.Fatalf("round trip = %+v, want %+v", got, c)
	}
}

func TestCursor_absentIsFirstLoad(t *testing.T) {
	got, err := readCursor(iceberg.Properties{"write.delete.mode": "merge-on-read"})
	if err != nil || got != nil {
		t.Fatalf("got (%v, %v), want (nil, nil): a table with no cursor keys is a first load", got, err)
	}
}

func TestCursor_partialIsCorrupt(t *testing.T) {
	_, err := readCursor(iceberg.Properties{propFile: "binlog.000001", propPos: "4"})
	if err == nil || !strings.Contains(err.Error(), "partial export cursor") {
		t.Fatalf("err = %v, want a partial-cursor refusal", err)
	}
	_, err = readCursor(iceberg.Properties{propFile: "binlog.000001", propPos: "four", propAt: "2026-08-28T12:00:00Z"})
	if err == nil || !strings.Contains(err.Error(), "not a position") {
		t.Fatalf("err = %v, want a bad-position refusal", err)
	}
}
