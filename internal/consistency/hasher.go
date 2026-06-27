package consistency

// Hasher is the exported, order-independent content-digest accumulator shared
// across the consistency epic (#631). ConsistentTableChecksum uses it to
// fingerprint a live source table; the baseline writer (#633) uses it to
// fingerprint the rows it ingests from a mydumper dump. Both route through the
// same internal rowHasher and the same version tag, so a live-source digest and
// a persisted baseline digest are byte-identical for the same data — which is
// what lets the verify capstone (#634) compare them meaningfully.
//
// A row is a list of field byte-slices in column order; a nil field is SQL NULL,
// distinct from a non-nil empty value. The digest is order-independent (rows may
// be added in any order) — see rowHasher for the construction and its 64-bit,
// non-cryptographic, accidental-corruption threat model.
type Hasher struct {
	rh *rowHasher
}

// NewHasher returns an empty Hasher.
func NewHasher() *Hasher { return &Hasher{rh: newRowHasher()} }

// AddStrings folds one row given the parallel value/null slices the baseline
// dump parser yields, in MySQL column order (matching ConsistentTableChecksum's
// ordinal SELECT order). A column is NULL when nulls[i] is true or absent — the
// same rule WriteRow applies — so the persisted digest reflects exactly the
// columns that reach the baseline. The string bytes are MySQL's text rendering
// (mydumper dumps what MySQL's text protocol returns), identical to the
// sql.RawBytes ConsistentTableChecksum hashes.
func (h *Hasher) AddStrings(values []string, nulls []bool) {
	row := make([][]byte, len(values))
	for i := range values {
		if i >= len(nulls) || nulls[i] {
			row[i] = nil
			continue
		}
		row[i] = []byte(values[i])
	}
	h.rh.add(row)
}

// Digest returns the version-tagged hex digest accumulated so far. An empty
// Hasher returns the version-tagged all-zero digest.
func (h *Hasher) Digest() string { return digestVersion + h.rh.digest() }

// Count returns the number of rows folded in.
func (h *Hasher) Count() int64 { return h.rh.count() }
