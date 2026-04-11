package reconstruct

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/recovery"
)

// ErrWriterClosed is returned from MydumperWriter methods called after Close.
// Kept as a sentinel so tests and future callers can match it with errors.Is.
var ErrWriterClosed = errors.New("MydumperWriter: already closed")

// MydumperWriter emits a full-table reconstruction as a mydumper-compatible
// SQL dump directory. Output layout:
//
//	<outputDir>/
//	├── metadata                         # one file for the whole run
//	├── <db>.<table>-schema.sql          # CREATE TABLE (from baseline metadata)
//	├── <db>.<table>.00000.sql           # INSERT statements, chunked
//	├── <db>.<table>.00001.sql
//	└── ...
//
// Each chunk file holds a single multi-row INSERT statement terminated by
// `;\n`, so concatenating `cat *.sql | mysql` works. When the accumulated
// chunk size passes the configured threshold the writer closes the current
// statement, opens the next chunk file, and starts a new INSERT.
type MydumperWriter struct {
	outputDir string
	schema    string
	table     string
	chunkSize int64

	// insertPrefix is the "INSERT INTO `db`.`tab` (`c1`, `c2`, ...) VALUES\n"
	// header emitted once at the top of each chunk file.
	insertPrefix string

	// cols is the column name list in the order WriteRow receives values.
	cols []string

	files []string // written file names, for TableReport

	curFile         *os.File
	curBuf          *bufio.Writer
	curBytes        int64
	chunkIdx        int
	firstRowInChunk bool
	closed          bool // true after Close; WriteRow becomes ErrWriterClosed
}

// NewMydumperWriter creates a writer for a single table. The directory must
// already exist; the caller is expected to pass a freshly-created output
// directory (e.g. from t.TempDir() or the CLI's --output-dir).
func NewMydumperWriter(outputDir, schema, table string, cols []string, chunkSize int64) (*MydumperWriter, error) {
	if chunkSize <= 0 {
		chunkSize = 256 << 20 // 256 MiB default
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("MydumperWriter: at least one column required")
	}

	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = recovery.QuoteName(c)
	}
	prefix := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES\n",
		recovery.QuoteName(schema),
		recovery.QuoteName(table),
		strings.Join(quotedCols, ", "),
	)

	return &MydumperWriter{
		outputDir:       outputDir,
		schema:          schema,
		table:           table,
		chunkSize:       chunkSize,
		cols:            cols,
		insertPrefix:    prefix,
		firstRowInChunk: true,
	}, nil
}

// WriteSchema emits the <db>.<table>-schema.sql file containing the exact
// CREATE TABLE text captured in baseline Parquet metadata. Called once, before
// any WriteRow.
func (w *MydumperWriter) WriteSchema(createSQL string) error {
	if w.closed {
		return ErrWriterClosed
	}
	name := fmt.Sprintf("%s.%s-schema.sql", w.schema, w.table)
	path := filepath.Join(w.outputDir, name)
	if err := os.WriteFile(path, []byte(createSQL), 0o644); err != nil {
		return fmt.Errorf("write schema file %s: %w", path, err)
	}
	w.files = append(w.files, name)
	return nil
}

// WriteRow appends one row tuple to the current chunk, rotating to a new
// chunk file when curBytes crosses chunkSize. values must be in the same
// order as the cols slice passed to NewMydumperWriter; len(values) must
// equal len(cols).
//
// Returns ErrWriterClosed if the writer has already been Close()d.
func (w *MydumperWriter) WriteRow(values []any) error {
	if w.closed {
		return ErrWriterClosed
	}
	if len(values) != len(w.cols) {
		return fmt.Errorf("MydumperWriter.WriteRow: got %d values for %d columns", len(values), len(w.cols))
	}

	// Open a new chunk file on first row, or after a previous rotation.
	if w.curFile == nil {
		if err := w.openChunk(); err != nil {
			return err
		}
	}

	// Format the tuple: (v1, v2, ...).
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = recovery.FormatSQLValue(v)
	}
	tuple := "(" + strings.Join(parts, ", ") + ")"

	// First row in chunk is preceded by the INSERT prefix (written in
	// openChunk). Subsequent rows are preceded by ",\n" to chain them into
	// the same multi-row INSERT.
	if w.firstRowInChunk {
		if _, err := w.curBuf.WriteString(tuple); err != nil {
			return err
		}
		w.curBytes += int64(len(tuple))
		w.firstRowInChunk = false
	} else {
		if _, err := w.curBuf.WriteString(",\n" + tuple); err != nil {
			return err
		}
		w.curBytes += int64(len(tuple)) + 2
	}

	// Rotate if the chunk has grown past the threshold.
	if w.curBytes >= w.chunkSize {
		if err := w.finishChunk(); err != nil {
			return err
		}
	}
	return nil
}

// Close terminates the in-progress INSERT (if any), flushes and closes the
// current chunk file, and marks the writer terminal so subsequent WriteRow
// calls return ErrWriterClosed. Idempotent.
func (w *MydumperWriter) Close() error {
	if w.closed {
		return nil
	}
	var err error
	if w.curFile != nil {
		err = w.finishChunk()
	}
	w.closed = true
	return err
}

// Files returns every file the writer has produced so far (schema + chunks),
// relative names only.
func (w *MydumperWriter) Files() []string {
	return w.files
}

// openChunk creates the next <db>.<table>.NNNNN.sql file and writes the
// INSERT prefix. Called on first WriteRow and after each rotation.
func (w *MydumperWriter) openChunk() error {
	name := fmt.Sprintf("%s.%s.%05d.sql", w.schema, w.table, w.chunkIdx)
	path := filepath.Join(w.outputDir, name)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create chunk file %s: %w", path, err)
	}
	w.curFile = f
	w.curBuf = bufio.NewWriterSize(f, 64<<10)
	w.curBytes = 0
	w.firstRowInChunk = true
	w.files = append(w.files, name)
	if _, err := w.curBuf.WriteString(w.insertPrefix); err != nil {
		return err
	}
	w.curBytes += int64(len(w.insertPrefix))
	return nil
}

// finishChunk terminates the current INSERT statement with ";\n", flushes
// the bufio.Writer, and closes the file. On any write / flush failure the
// partially-written chunk file is removed so downstream `cat *.sql | mysql`
// never sees a half-written statement.
func (w *MydumperWriter) finishChunk() error {
	if w.curFile == nil {
		return nil
	}

	// Capture the chunk's filesystem path up-front so we can remove it on
	// error (w.files[last] holds the relative name).
	chunkPath := filepath.Join(w.outputDir, w.files[len(w.files)-1])

	// removeChunk drops the partial file from disk AND from the files list.
	// Called on any error below so callers never see a dangling chunk.
	removeChunk := func() {
		if rmErr := os.Remove(chunkPath); rmErr != nil && !os.IsNotExist(rmErr) {
			// Best-effort — log but don't shadow the original error. Tests
			// rely on the file list accurately reflecting what's on disk.
			_ = rmErr
		}
		w.files = w.files[:len(w.files)-1]
	}

	// Only emit the trailing semicolon if we actually wrote at least one row.
	// An empty INSERT prefix with nothing after it would be a SQL error.
	if !w.firstRowInChunk {
		if _, err := w.curBuf.WriteString(";\n"); err != nil {
			_ = w.curFile.Close()
			removeChunk()
			w.resetChunkState()
			return fmt.Errorf("finishChunk: write terminator: %w", err)
		}
	}
	if err := w.curBuf.Flush(); err != nil {
		_ = w.curFile.Close()
		removeChunk()
		w.resetChunkState()
		return fmt.Errorf("finishChunk: flush: %w", err)
	}
	if err := w.curFile.Close(); err != nil {
		removeChunk()
		w.resetChunkState()
		return fmt.Errorf("finishChunk: close: %w", err)
	}

	// Successful close: if the chunk was empty (INSERT prefix only, no
	// rows), drop the file so downstream restore doesn't see a dangling
	// "INSERT INTO ... VALUES" statement.
	if w.firstRowInChunk {
		removeChunk()
	}

	w.resetChunkState()
	return nil
}

// resetChunkState clears the per-chunk cursor state after a successful or
// failed chunk close, preparing the writer for the next openChunk call.
func (w *MydumperWriter) resetChunkState() {
	w.curFile = nil
	w.curBuf = nil
	w.curBytes = 0
	w.chunkIdx++
}

// WriteMetadataFile writes a mydumper-compatible top-level "metadata" file
// that baseline.ParseMetadata can read back. The format is symmetric to what
// mydumper produces so myloader recognizes it.
//
// outputDir is the same directory passed to NewMydumperWriter. at is the
// target point-in-time of the reconstruction. binlogFile / binlogPos / gtid
// should come from baseline.ReadParquetMetadataAny so the metadata file
// reflects the baseline's replication position, not some synthetic value.
func WriteMetadataFile(outputDir string, at time.Time, gtid, binlogFile string, binlogPos int64) error {
	path := filepath.Join(outputDir, "metadata")
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Started dump at: %s\n", time.Now().UTC().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&sb, "# Target timestamp: %s\n", at.UTC().Format(time.RFC3339))
	sb.WriteString("# Produced by bintrail reconstruct --output-format mydumper\n\n")

	sb.WriteString("[config]\n")
	sb.WriteString("quote_character = `\n\n")

	sb.WriteString("[myloader_session_variables]\n\n")

	sb.WriteString("[master]\n")
	if binlogFile != "" {
		fmt.Fprintf(&sb, "\tLog: %s\n", binlogFile)
	}
	if binlogPos != 0 {
		fmt.Fprintf(&sb, "\tPos: %d\n", binlogPos)
	}
	if gtid != "" {
		fmt.Fprintf(&sb, "\tGTID: %s\n", gtid)
	}

	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		return fmt.Errorf("write metadata file %s: %w", path, err)
	}
	return nil
}
