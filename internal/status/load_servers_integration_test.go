//go:build integration

package status_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/dbtrail/bintrail/internal/serverid"
	"github.com/dbtrail/bintrail/internal/status"
	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestLoadServers_integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)

	_, err := db.Exec(serverid.DDLBintrailServers)
	if err != nil {
		t.Fatalf("create bintrail_servers: %v", err)
	}

	_, err = db.Exec(`INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username)
		VALUES ('97adaf56-fe9e-4c1b-9794-b042f7faf197', '55512139-1432-11f1-8d8d-0693b428a89b', 'testhost.rds.amazonaws.com', 3306, 'admin')`)
	if err != nil {
		t.Fatalf("insert test server: %v", err)
	}

	servers, err := status.LoadServers(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadServers failed: %v", err)
	}

	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
	if servers[0].BintrailID != "97adaf56-fe9e-4c1b-9794-b042f7faf197" {
		t.Errorf("wrong bintrail_id: %s", servers[0].BintrailID)
	}
	if servers[0].Port != 3306 {
		t.Errorf("wrong port: %d", servers[0].Port)
	}

	// Verify WriteStatus actually renders the Servers section
	var buf bytes.Buffer
	status.WriteStatus(&buf, nil, nil, nil, nil, servers, nil)
	out := buf.String()
	if !strings.Contains(out, "=== Servers ===") {
		t.Errorf("expected Servers section in output:\n%s", out)
	}
	if !strings.Contains(out, "97adaf56-fe9e-4c1b-9794-b042f7faf197") {
		t.Errorf("expected bintrail_id in output:\n%s", out)
	}
}

func TestLoadStreamState_integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ctx := context.Background()

	// Empty table → nil, no error
	stream, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState on empty table: %v", err)
	}
	if stream != nil {
		t.Fatal("expected nil for empty stream_state")
	}

	// Insert a stream state row
	_, err = db.Exec(`INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set,
		 events_indexed, last_event_time, last_checkpoint, server_id, bintrail_id)
		VALUES (1, 'gtid', 'binlog.000005', 12345, 'aaa-bbb:1-100',
		        267354, '2026-03-03 16:56:40', '2026-03-03 16:56:45', 42, '97adaf56-fe9e-4c1b-9794-b042f7faf197')`)
	if err != nil {
		t.Fatalf("insert stream_state: %v", err)
	}

	stream, err = status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream state")
	}

	if stream.Mode != "gtid" {
		t.Errorf("wrong mode: %s", stream.Mode)
	}
	if stream.BinlogFile != "binlog.000005" {
		t.Errorf("wrong binlog_file: %s", stream.BinlogFile)
	}
	if stream.BinlogPosition != 12345 {
		t.Errorf("wrong binlog_position: %d", stream.BinlogPosition)
	}
	if !stream.GTIDSet.Valid || stream.GTIDSet.String != "aaa-bbb:1-100" {
		t.Errorf("wrong gtid_set: %v", stream.GTIDSet)
	}
	if stream.EventsIndexed != 267354 {
		t.Errorf("wrong events_indexed: %d", stream.EventsIndexed)
	}
	if stream.ServerID != 42 {
		t.Errorf("wrong server_id: %d", stream.ServerID)
	}
	if !stream.BintrailID.Valid || stream.BintrailID.String != "97adaf56-fe9e-4c1b-9794-b042f7faf197" {
		t.Errorf("wrong bintrail_id: %v", stream.BintrailID)
	}

	// Verify WriteStatus renders the Stream section with bintrail-id
	var buf bytes.Buffer
	status.WriteStatus(&buf, nil, nil, nil, nil, nil, stream)
	out := buf.String()
	if !strings.Contains(out, "=== Stream ===") {
		t.Errorf("expected Stream section in output:\n%s", out)
	}
	if !strings.Contains(out, "97adaf56-fe9e-4c1b-9794-b042f7faf197") {
		t.Errorf("expected bintrail_id in output:\n%s", out)
	}
	if !strings.Contains(out, "267354") {
		t.Errorf("expected events_indexed in output:\n%s", out)
	}
}
