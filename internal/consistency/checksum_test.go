package consistency

import "testing"

// digestOf builds a multiset digest from a list of rows, where each row is a
// list of fields and a nil field represents SQL NULL.
func digestOf(rows [][][]byte) (string, int64) {
	h := newRowHasher()
	for _, row := range rows {
		h.add(row)
	}
	return h.digest(), h.count()
}

func b(s string) []byte { return []byte(s) }

func TestRowHasher_OrderIndependent(t *testing.T) {
	rows := [][][]byte{
		{b("1"), b("alice")},
		{b("2"), b("bob")},
		{b("3"), b("carol")},
	}
	shuffled := [][][]byte{
		{b("3"), b("carol")},
		{b("1"), b("alice")},
		{b("2"), b("bob")},
	}
	d1, c1 := digestOf(rows)
	d2, c2 := digestOf(shuffled)
	if d1 != d2 {
		t.Errorf("digest depends on row order: %s != %s", d1, d2)
	}
	if c1 != 3 || c2 != 3 {
		t.Errorf("count = (%d,%d), want (3,3)", c1, c2)
	}
}

func TestRowHasher_SingleByteChangeDiffers(t *testing.T) {
	base := [][][]byte{{b("1"), b("alice")}}
	changed := [][][]byte{{b("1"), b("alicf")}} // one byte different
	d1, _ := digestOf(base)
	d2, _ := digestOf(changed)
	if d1 == d2 {
		t.Errorf("single-byte change did not change digest: both %s", d1)
	}
}

func TestRowHasher_NullDistinctFromEmpty(t *testing.T) {
	withNull := [][][]byte{{b("1"), nil}}
	withEmpty := [][][]byte{{b("1"), b("")}}
	d1, _ := digestOf(withNull)
	d2, _ := digestOf(withEmpty)
	if d1 == d2 {
		t.Errorf("NULL and empty value hashed the same: both %s", d1)
	}
}

func TestRowHasher_FieldBoundaryUnambiguous(t *testing.T) {
	// Without length-prefixing, ("ab","c") and ("a","bc") would concatenate to
	// the same bytes. They must differ.
	a := [][][]byte{{b("ab"), b("c")}}
	c := [][][]byte{{b("a"), b("bc")}}
	d1, _ := digestOf(a)
	d2, _ := digestOf(c)
	if d1 == d2 {
		t.Errorf("ambiguous field boundary: ('ab','c') and ('a','bc') hashed the same: %s", d1)
	}
}

func TestRowHasher_DuplicateRowsDoNotCancel(t *testing.T) {
	// Two identical rows must not XOR-cancel to the empty digest; addition keeps
	// them. The digest of two identical rows differs from one.
	one := [][][]byte{{b("x")}}
	two := [][][]byte{{b("x")}, {b("x")}}
	d1, c1 := digestOf(one)
	d2, c2 := digestOf(two)
	if c1 != 1 || c2 != 2 {
		t.Fatalf("counts = (%d,%d), want (1,2)", c1, c2)
	}
	if d2 == d1 {
		t.Errorf("two identical rows hashed same as one: %s", d2)
	}
	empty, _ := digestOf(nil)
	if d2 == empty {
		t.Errorf("two identical rows cancelled to empty digest %s", empty)
	}
}

func TestRowHasher_EmptyTable(t *testing.T) {
	d, c := digestOf(nil)
	if c != 0 {
		t.Errorf("empty count = %d, want 0", c)
	}
	if d != "0000000000000000" {
		t.Errorf("empty digest = %s, want all-zero", d)
	}
}

func TestDigestVersionOf(t *testing.T) {
	cases := map[string]string{
		"v2:0011223344556677": "v2:", // current contract tag
		"v1:deadbeef":         "v1:", // a persisted pre-pin baseline digest
		"":                    "",    // empty
		"deadbeef":            "",    // untagged legacy value (no ':')
		":abc":                ":",   // degenerate but tagged
	}
	for in, want := range cases {
		if got := DigestVersionOf(in); got != want {
			t.Errorf("DigestVersionOf(%q) = %q, want %q", in, got, want)
		}
	}
	// The exported contract tag must round-trip through DigestVersionOf, so a
	// consumer comparing a freshly computed digest can detect its own version.
	if got := DigestVersionOf(DigestVersion + "0000000000000000"); got != DigestVersion {
		t.Errorf("DigestVersionOf(current digest) = %q, want %q", got, DigestVersion)
	}
	// Pin the #792 bump so an accidental revert to v1 is caught.
	if DigestVersion != "v2:" {
		t.Errorf("DigestVersion = %q, want \"v2:\" (charset-pin contract, #792)", DigestVersion)
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"id":       "`id`",
		"weird`col": "`weird``col`",
		"":         "``",
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}
