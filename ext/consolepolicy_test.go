package ext

import (
	"reflect"
	"testing"
)

// TestSessionRestrictionsEmpty pins the nil-safety of Empty: a nil or
// zero-value restriction set restricts nothing.
func TestSessionRestrictionsEmpty(t *testing.T) {
	var nilR *SessionRestrictions
	if !nilR.Empty() {
		t.Error("nil *SessionRestrictions must be Empty")
	}
	if !(&SessionRestrictions{}).Empty() {
		t.Error("zero-value SessionRestrictions must be Empty")
	}
}

// TestSessionRestrictionsEmptyCoversEveryField derives the per-field cases by
// reflection instead of hand-listing them: for EVERY slice field of the
// struct, a value with only that field populated must be non-Empty. The
// hazard this guards is a field added later and forgotten in Empty — such a
// restriction would silently read as unrestricted, which is the direction
// that serves data it should not.
func TestSessionRestrictionsEmptyCoversEveryField(t *testing.T) {
	typ := reflect.TypeOf(SessionRestrictions{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		v := reflect.New(typ).Elem()
		fv := v.Field(i)
		if fv.Kind() != reflect.Slice {
			t.Fatalf("SessionRestrictions grew non-slice field %s; teach Empty (and this test) its emptiness rule", f.Name)
		}
		fv.Set(reflect.Append(fv, reflect.New(fv.Type().Elem()).Elem()))
		if v.Addr().Interface().(*SessionRestrictions).Empty() {
			t.Errorf("SessionRestrictions{%s: one element}.Empty() = true — Empty was not taught about this field", f.Name)
		}
	}
}

// TestDataRestricted pins the console gate predicate: a session is
// data-restricted by a profile name, by direct restrictions, or by both — and
// NOT by permissions alone, nor by a non-nil-but-empty restriction struct (a
// provider that constructs the struct unconditionally must not accidentally
// lock its full-access sessions out of the raw-data surfaces).
func TestDataRestricted(t *testing.T) {
	cases := []struct {
		name string
		pol  *AccessPolicy
		want bool
	}{
		{"nil policy", nil, false},
		{"permissions only", &AccessPolicy{Permissions: AllPermissions()}, false},
		{"empty restrictions struct", &AccessPolicy{Restrictions: &SessionRestrictions{}}, false},
		{"profile", &AccessPolicy{Profile: "sensitive"}, true},
		{"restrictions", &AccessPolicy{Restrictions: &SessionRestrictions{DenyTables: []TableRef{{Schema: "s", Table: "t"}}}}, true},
		{"profile and restrictions", &AccessPolicy{Profile: "p", Restrictions: &SessionRestrictions{AllowTables: []TableRef{{Schema: "s", Table: "t"}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pol.DataRestricted(); got != tc.want {
				t.Errorf("DataRestricted = %v, want %v", got, tc.want)
			}
		})
	}
}
