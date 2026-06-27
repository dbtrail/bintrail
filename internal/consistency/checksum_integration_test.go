//go:build integration

package consistency

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestConsistentTableChecksum_BasicAndDeterministic(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(10,2))")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1,'alice',1.50),(2,'bob',2.00),(3,'carol',3.25)")

	ctx := context.Background()
	c1, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if c1.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", c1.RowCount)
	}
	if c1.Digest == "" || c1.Digest == "0000000000000000" {
		t.Errorf("Digest = %q, want a non-empty non-zero hash", c1.Digest)
	}

	// Recomputing over identical data yields an identical digest.
	c2, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum (2nd): %v", err)
	}
	if c1.Digest != c2.Digest {
		t.Errorf("digest not deterministic: %s != %s", c1.Digest, c2.Digest)
	}
}

func TestConsistentTableChecksum_OrderIndependent(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	// Same rows, inserted in different physical order.
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1,'alice'),(2,'bob'),(3,'carol')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (3,'carol'),(1,'alice'),(2,'bob')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("digest depends on insertion order: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_SingleByteChangeDiffers(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1,'alice')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1,'alicf')") // one byte different

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("single-byte change did not change digest: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_RepresentationOnlyDiffMatches(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, doc JSON)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, doc JSON)")
	// Same logical JSON, different source whitespace/key order; the server
	// normalizes both to the same canonical text form.
	testutil.MustExec(t, db, `INSERT INTO a VALUES (1, '{"a": 1, "b": 2}')`)
	testutil.MustExec(t, db, `INSERT INTO b VALUES (1, '{"b":2,"a":1}')`)

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("representation-only JSON difference changed digest: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_GeneratedColumnsExcluded(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// Table b has an extra STORED generated column derived from base columns.
	// Since mydumper omits generated columns, the checksum must ignore it, so
	// a and b must match.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, price INT, qty INT)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, price INT, qty INT, total INT AS (price*qty) STORED)")
	testutil.MustExec(t, db, "INSERT INTO a (id,price,qty) VALUES (1,10,2),(2,5,4)")
	testutil.MustExec(t, db, "INSERT INTO b (id,price,qty) VALUES (1,10,2),(2,5,4)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("generated column affected digest: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_UnsignedRenderedUnsigned(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// A BIGINT UNSIGNED value above 2^63 must render unsigned in the digest
	// (the classic UNSIGNED⇒negative corruption class, #490). We assert the
	// checksum succeeds and the row is counted; the value is captured as its
	// unsigned text form by the text protocol.
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1, 18446744073709551615)")

	ctx := context.Background()
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if c.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", c.RowCount)
	}
}

func TestConsistentTableChecksum_NullDistinctFromEmpty(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, NULL)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '')")

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("NULL and empty string hashed the same: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_EmptyTable(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64))")

	ctx := context.Background()
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if c.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0", c.RowCount)
	}
	if c.Digest != "0000000000000000" {
		t.Errorf("Digest = %q, want all-zero for empty table", c.Digest)
	}
}

func TestConsistentTableChecksum_MissingTableErrors(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	ctx := context.Background()
	if _, err := ConsistentTableChecksum(ctx, db, schema, "does_not_exist"); err == nil {
		t.Error("expected error for missing table, got nil")
	}
}

func TestConsistentTableChecksum_CapturesGTIDWhenEnabled(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1)")

	var gtidMode string
	if err := db.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		t.Skipf("cannot read @@gtid_mode: %v", err)
	}

	ctx := context.Background()
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if gtidMode == "ON" && c.GTIDSet == "" {
		t.Error("gtid_mode=ON but GTIDSet is empty")
	}
}
