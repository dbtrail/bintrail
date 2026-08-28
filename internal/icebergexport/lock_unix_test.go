//go:build unix

package icebergexport

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLockWarehouse_secondWriterRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), lockFileName)
	release, err := lockWarehouse(p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lockWarehouse(p)
	if err == nil || !strings.Contains(err.Error(), "another Iceberg export is running") {
		release()
		t.Fatalf("second lock: err = %v, want the single-writer refusal", err)
	}
	release()
	release2, err := lockWarehouse(p)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release2()
}
