package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// classedErr is a stand-in for a producer package's error: it declares its
// class through the Classed interface and carries a message that must never
// reach the wire.
type classedErr struct{ class, msg string }

func (e *classedErr) Error() string          { return e.msg }
func (e *classedErr) TelemetryClass() string { return e.class }

func TestClassifyErrorClassed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"declared class wins", &classedErr{class: ClassConfigInvalid}, ClassConfigInvalid},
		{"declared class survives wrapping", fmt.Errorf("preflight failed: %w", &classedErr{class: ClassConfigInvalid}), ClassConfigInvalid},
		{"declared class beats a wrapped MySQL number", &wrapBoth{&classedErr{class: ClassSchemaMismatch}, &mysql.MySQLError{Number: 1045}}, ClassSchemaMismatch},
		// A producer that spells a class wrong must not put free text on the
		// wire: normalizeClass coerces it, and the wiring test in that
		// producer's package is what catches the typo.
		{"typo is coerced to unknown", &classedErr{class: "config-invalid"}, ClassUnknown},
		{"empty class is coerced to unknown", &classedErr{class: ""}, ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}

// wrapBoth exposes two errors to errors.As at once, the way a chain built
// with fmt.Errorf("%w ... %w") does.
type wrapBoth struct{ a, b error }

func (w *wrapBoth) Error() string   { return w.a.Error() + "; " + w.b.Error() }
func (w *wrapBoth) Unwrap() []error { return []error{w.a, w.b} }

// numberedErr stands in for parser.ReplicationError: a server error number
// arriving from the replication client, which this package must not link.
type numberedErr struct {
	code uint16
	msg  string
}

func (e *numberedErr) Error() string            { return e.msg }
func (e *numberedErr) MySQLErrorNumber() uint16 { return e.code }

// TestClassifyErrorReplicationClient: a binlog-side failure never arrives as
// the driver's *MySQLError — it comes from the replication client through
// MySQLNumbered — and 1236 is the one number a capture daemon actually dies
// on. Before #1503 the classifier only looked at the driver's type, so every
// replication-side server error was unknown.
func TestClassifyErrorReplicationClient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"binlog purged (1236)", &numberedErr{code: 1236}, ClassBinlogNotFound},
		{"1236 through the driver type", &mysql.MySQLError{Number: 1236}, ClassBinlogNotFound},
		{"host not allowed (1130)", &numberedErr{code: 1130}, ClassDBPermission},
		{"1130 through the driver type", &mysql.MySQLError{Number: 1130}, ClassDBPermission},
		{"access denied via replication client", &numberedErr{code: 1045}, ClassDBPermission},
		{"unbucketed number via replication client", &numberedErr{code: 1064}, ClassUnknown},
		{"wrapped replication error", fmt.Errorf("stream: %w", &numberedErr{code: 1236}), ClassBinlogNotFound},
		{"cancelled context stays unknown", context.Canceled, ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}

// TestClassifyErrorNeverLeaksClassedOrReplicationMessage extends the
// load-bearing no-leak property to the two new inputs.
func TestClassifyErrorNeverLeaksClassedOrReplicationMessage(t *testing.T) {
	secret := "mysql-bin.000042 on db.internal for customer_orders"
	for name, err := range map[string]error{
		"classed":     &classedErr{class: ClassBinlogNotFound, msg: secret},
		"replication": &numberedErr{code: 1236, msg: secret},
	} {
		got := ClassifyError(fmt.Errorf("gap: %s: %w", secret, err))
		if strings.Contains(got, "000042") || strings.Contains(got, "db.internal") || strings.Contains(got, "customer_orders") {
			t.Fatalf("%s: ClassifyError leaked the error message: %q", name, got)
		}
		if !classes[got] {
			t.Fatalf("%s: ClassifyError returned %q, outside the taxonomy", name, got)
		}
	}
}

// TestDroppedClassesAreGone pins #1503's removal: a class with no producer
// must not be reintroduced without one.
func TestDroppedClassesAreGone(t *testing.T) {
	for _, dropped := range []string{"binlog_parse", "flag_invalid", "network"} {
		if classes[dropped] {
			t.Errorf("class %q is back in the taxonomy; it was removed because nothing can emit it", dropped)
		}
		if got := normalizeClass(dropped); got != ClassUnknown {
			t.Errorf("normalizeClass(%q) = %q, want unknown", dropped, got)
		}
	}
	if _, ok := any(errors.New("x")).(Classed); ok {
		t.Fatal("a plain error must not satisfy Classed")
	}
}
