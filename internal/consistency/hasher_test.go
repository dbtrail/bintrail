package consistency

import "testing"

// expectedDigest computes the digest the same way ConsistentTableChecksum does
// (private rowHasher fed []byte rows, version-tagged), so the exported Hasher can
// be proven byte-identical to it.
func expectedDigest(rows [][][]byte) string {
	rh := newRowHasher()
	for _, r := range rows {
		rh.add(r)
	}
	return digestVersion + rh.digest()
}

func TestHasher_AddStringsMatchesRawBytesPath(t *testing.T) {
	// The same logical row fed as strings (baseline path) and as raw bytes
	// (ConsistentTableChecksum path) must yield the same digest.
	values := []string{"1", "café", "18446744073709551615", ""}
	nulls := []bool{false, false, false, false}

	h := NewHasher()
	h.AddStrings(values, nulls)
	got := h.Digest()

	want := expectedDigest([][][]byte{{[]byte("1"), []byte("café"), []byte("18446744073709551615"), []byte("")}})
	if got != want {
		t.Errorf("AddStrings digest %s != raw-bytes digest %s", got, want)
	}
}

func TestHasher_NullDistinctFromEmpty(t *testing.T) {
	withNull := NewHasher()
	withNull.AddStrings([]string{"1", "x"}, []bool{false, true})

	withEmpty := NewHasher()
	withEmpty.AddStrings([]string{"1", ""}, []bool{false, false})

	if withNull.Digest() == withEmpty.Digest() {
		t.Errorf("NULL and empty hashed the same: %s", withNull.Digest())
	}
	// And the NULL path must match a raw nil element exactly.
	want := expectedDigest([][][]byte{{[]byte("1"), nil}})
	if withNull.Digest() != want {
		t.Errorf("AddStrings NULL digest %s != raw nil digest %s", withNull.Digest(), want)
	}
}

func TestHasher_NullsShorterThanValuesIsNull(t *testing.T) {
	// A column index past the end of nulls is treated as NULL (mirrors WriteRow).
	short := NewHasher()
	short.AddStrings([]string{"1", "ignored"}, []bool{false}) // index 1 absent → NULL

	want := expectedDigest([][][]byte{{[]byte("1"), nil}})
	if short.Digest() != want {
		t.Errorf("out-of-range null not treated as NULL: %s != %s", short.Digest(), want)
	}
}

func TestHasher_OrderIndependentAndCounted(t *testing.T) {
	a := NewHasher()
	a.AddStrings([]string{"1", "alice"}, []bool{false, false})
	a.AddStrings([]string{"2", "bob"}, []bool{false, false})

	b := NewHasher()
	b.AddStrings([]string{"2", "bob"}, []bool{false, false})
	b.AddStrings([]string{"1", "alice"}, []bool{false, false})

	if a.Digest() != b.Digest() {
		t.Errorf("digest depends on row order: %s != %s", a.Digest(), b.Digest())
	}
	if a.Count() != 2 || b.Count() != 2 {
		t.Errorf("count = (%d,%d), want (2,2)", a.Count(), b.Count())
	}
}

func TestHasher_ZeroDateHashedAsStringNotNull(t *testing.T) {
	// The tap must hash a MySQL zero-date as its non-null string — matching the
	// live checksum's CAST(... AS CHAR) — even though the baseline writer stores
	// it as Parquet NULL. Mirroring the writer here would false-mismatch every
	// zero-date table against the source.
	zeroDate := NewHasher()
	zeroDate.AddStrings([]string{"1", "0000-00-00 00:00:00"}, []bool{false, false})

	asNull := NewHasher()
	asNull.AddStrings([]string{"1", "x"}, []bool{false, true})

	if zeroDate.Digest() == asNull.Digest() {
		t.Errorf("zero-date hashed as NULL instead of its string: %s", zeroDate.Digest())
	}
	want := expectedDigest([][][]byte{{[]byte("1"), []byte("0000-00-00 00:00:00")}})
	if zeroDate.Digest() != want {
		t.Errorf("zero-date digest %s != raw-string digest %s", zeroDate.Digest(), want)
	}
}

func TestHasher_VersionTaggedAndEmpty(t *testing.T) {
	h := NewHasher()
	if got := h.Digest(); got != digestVersion+"0000000000000000" {
		t.Errorf("empty digest = %q, want version-tagged all-zero", got)
	}
	if h.Count() != 0 {
		t.Errorf("empty count = %d, want 0", h.Count())
	}
}
