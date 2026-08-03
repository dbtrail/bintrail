package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMydumperWriterRowsAcrossRotation pins the "exact by construction"
// claim where it could actually drift: chunk rotation. A tiny chunkSize
// forces a rotation on every row; Rows() must equal both the tuples
// accepted AND the tuples present on disk across all chunks.
func TestMydumperWriterRowsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "s", "t", []string{"id"}, 4 /* bytes: rotate every row */)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if err := w.WriteRow([]any{int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Rows() != n {
		t.Fatalf("Rows() = %d, want %d", w.Rows(), n)
	}
	chunks := 0
	tuples := 0
	for _, name := range w.Files() {
		if strings.HasSuffix(name, "-schema.sql") {
			continue
		}
		chunks++
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		// Each chunk holds one "(`id`) VALUES" header paren plus one paren
		// per tuple.
		tuples += strings.Count(string(b), "(") - 1
	}
	if chunks < 2 {
		t.Fatalf("chunkSize=4 must force rotation, got %d chunk(s)", chunks)
	}
	if tuples != n {
		t.Fatalf("tuples on disk = %d, want %d (counter drifted from disk)", tuples, n)
	}
}
