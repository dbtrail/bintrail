// Package accessprofiles is the ONE implementation of authoring access
// profiles: the flags on tables and columns (table_flags), the named
// profiles (profiles) and the allow/deny rules that join them
// (access_rules). The CLI verbs (`bintrail flag|profile|access add|remove|
// list`) and the console's Access profiles page both call these functions,
// so a profile authored from either surface is the same rows, refused for
// the same reasons, with the same words (#1445). Presentation stays with
// the caller: the CLI prints its own success and not-found lines, the
// console maps the typed errors below onto HTTP statuses.
//
// The read side of the same tables (LoadProfileRules, ProfileExists,
// ListProfiles) lives in internal/query, next to the enforcement it feeds.
package accessprofiles

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Flag is one row of table_flags: a named label on a table (Column == "")
// or on one of its columns.
type Flag struct {
	Schema    string
	Table     string
	Column    string
	Name      string
	CreatedAt time.Time
}

// Profile is one row of profiles.
type Profile struct {
	Name        string
	Description string
	CreatedAt   time.Time
}

// Rule is one row of access_rules, keyed by the profile's NAME rather than
// its id: the id is an implementation detail neither surface shows.
type Rule struct {
	Profile    string
	Flag       string
	Permission string // PermissionAllow or PermissionDeny
	CreatedAt  time.Time
}

// The two permission values access_rules accepts (its ENUM).
const (
	PermissionAllow = "allow"
	PermissionDeny  = "deny"
)

// Trimmed returns f with the surrounding whitespace taken off every name.
// Every writer applies it first, so "marketing " and "marketing" are one
// row on both surfaces; the CLI calls it too so its printed lines show the
// value that was stored.
func (f Flag) Trimmed() Flag {
	f.Schema, f.Table, f.Column, f.Name =
		strings.TrimSpace(f.Schema), strings.TrimSpace(f.Table), strings.TrimSpace(f.Column), strings.TrimSpace(f.Name)
	return f
}

// Trimmed returns p with the surrounding whitespace taken off its name and
// description.
func (p Profile) Trimmed() Profile {
	p.Name, p.Description = strings.TrimSpace(p.Name), strings.TrimSpace(p.Description)
	return p
}

// Trimmed returns r with the surrounding whitespace taken off every field.
func (r Rule) Trimmed() Rule {
	r.Profile, r.Flag, r.Permission = strings.TrimSpace(r.Profile), strings.TrimSpace(r.Flag), strings.TrimSpace(r.Permission)
	return r
}

// MissingFieldError is a required value that was empty. Field is named as
// the operator sees it ("schema", "flag name").
type MissingFieldError struct{ Field string }

func (e *MissingFieldError) Error() string { return e.Field + " is required" }

// InvalidPermissionError is a permission other than allow or deny. The
// message names the field; the CLI prefixes the flag dashes itself so its
// wording stays exactly what it was.
type InvalidPermissionError struct{ Got string }

func (e *InvalidPermissionError) Error() string {
	return fmt.Sprintf("permission must be %q or %q, got %q", PermissionAllow, PermissionDeny, e.Got)
}

// ProfileNotFoundError names a profile that does not exist on this index.
type ProfileNotFoundError struct{ Name string }

func (e *ProfileNotFoundError) Error() string { return fmt.Sprintf("profile %q not found", e.Name) }

// FlagNotFoundError is a RemoveFlag that matched no row.
type FlagNotFoundError struct{ Flag Flag }

func (e *FlagNotFoundError) Error() string {
	msg := fmt.Sprintf("flag %q not found on %s.%s", e.Flag.Name, e.Flag.Schema, e.Flag.Table)
	if e.Flag.Column != "" {
		msg += " (" + e.Flag.Column + ")"
	}
	return msg
}

// RuleNotFoundError is a RemoveRule that matched no row.
type RuleNotFoundError struct{ Profile, Flag string }

func (e *RuleNotFoundError) Error() string {
	return fmt.Sprintf("access rule not found: profile=%q flag=%q", e.Profile, e.Flag)
}

// collationNote is the tail of the two "already exists" refusals: the
// index's default collation (utf8mb4_0900_ai_ci) folds case AND accents, so
// "Marketing", "marketing" and "márketing" are one row to its unique keys.
const collationNote = " (the index compares names without regard to case or accents)"

// ProfileExistsError is an AddProfile whose name differs only by what the
// index's collation ignores (letter case, accents) from a profile that
// exists. The unique key on profiles.name folds those, so the INSERT would
// silently update the existing row instead of creating a second profile;
// both surfaces refuse instead, naming the row that is there.
type ProfileExistsError struct{ Existing, Requested string }

func (e *ProfileExistsError) Error() string {
	return fmt.Sprintf("a profile named %q already exists", e.Existing) + collationNote
}

// FlagExistsError is an AddFlag whose (flag, schema, table, column) differs
// only by what the collation ignores from a row that exists: the INSERT's
// ON DUPLICATE KEY would leave the stored spelling and report success, so
// the caller would be told "PII" was added while the row still says "pii".
// Existing is the stored row.
type FlagExistsError struct{ Existing, Requested Flag }

func (e *FlagExistsError) Error() string {
	msg := fmt.Sprintf("flag %q already exists on %s.%s", e.Existing.Name, e.Existing.Schema, e.Existing.Table)
	if e.Existing.Column != "" {
		msg += " (" + e.Existing.Column + ")"
	}
	return msg + collationNote
}

// The column widths of the three tables (internal/indexer/schema.go):
// schema, table and column names are VARCHAR(64) like MySQL's own
// identifiers, flag and profile names VARCHAR(255), the description a TEXT.
// A value past its width would reach the database and come back as a raw
// 1406 "data too long" error; the writers refuse first, naming the limit.
const (
	MaxIdentifierLen  = 64    // schema, table and column names, in characters
	MaxFlagLen        = 255   // flag names, in characters
	MaxProfileNameLen = 255   // profile names, in characters
	MaxDescriptionLen = 65535 // profile descriptions, in bytes (TEXT)
)

// TooLongError is a value past its column's width. Unit is "characters" for
// the VARCHAR columns and "bytes" for the TEXT description.
type TooLongError struct {
	Field string
	Got   int
	Max   int
	Unit  string
}

func (e *TooLongError) Error() string {
	return fmt.Sprintf("%s is too long (%d %s); the limit is %d %s", e.Field, e.Got, e.Unit, e.Max, e.Unit)
}

func checkLen(field, value string, max int) error {
	if n := utf8.RuneCountInString(value); n > max {
		return &TooLongError{Field: field, Got: n, Max: max, Unit: "characters"}
	}
	return nil
}

// IsRefusal reports whether err is one of this package's typed refusals
// (bad input or a missing row) rather than a database failure. Callers use
// it to pick a 4xx over a 500 without listing every type.
func IsRefusal(err error) bool {
	var mf *MissingFieldError
	var ip *InvalidPermissionError
	var tl *TooLongError
	var pe *ProfileExistsError
	var fe *FlagExistsError
	var pnf *ProfileNotFoundError
	var fnf *FlagNotFoundError
	var rnf *RuleNotFoundError
	return errors.As(err, &mf) || errors.As(err, &ip) || errors.As(err, &tl) || errors.As(err, &pe) ||
		errors.As(err, &fe) || errors.As(err, &pnf) || errors.As(err, &fnf) || errors.As(err, &rnf)
}

// IsConflict reports whether err says the row to add is already there under
// another spelling (*ProfileExistsError, *FlagExistsError).
func IsConflict(err error) bool {
	var pe *ProfileExistsError
	var fe *FlagExistsError
	return errors.As(err, &pe) || errors.As(err, &fe)
}

// IsNotFound reports whether err says the row to remove was not there.
func IsNotFound(err error) bool {
	var pnf *ProfileNotFoundError
	var fnf *FlagNotFoundError
	var rnf *RuleNotFoundError
	return errors.As(err, &pnf) || errors.As(err, &fnf) || errors.As(err, &rnf)
}

// DBExecer is the subset of *sql.DB (or *sql.Tx) the writers need.
type DBExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func requireFlagKey(f Flag) error {
	switch {
	case f.Name == "":
		return &MissingFieldError{Field: "flag name"}
	case f.Schema == "":
		return &MissingFieldError{Field: "schema"}
	case f.Table == "":
		return &MissingFieldError{Field: "table"}
	}
	for _, c := range []struct {
		field, value string
		max          int
	}{
		{"flag name", f.Name, MaxFlagLen},
		{"schema", f.Schema, MaxIdentifierLen},
		{"table", f.Table, MaxIdentifierLen},
		{"column", f.Column, MaxIdentifierLen},
	} {
		if err := checkLen(c.field, c.value, c.max); err != nil {
			return err
		}
	}
	return nil
}

func requireProfileName(name string) error {
	if name == "" {
		return &MissingFieldError{Field: "profile name"}
	}
	return checkLen("profile name", name, MaxProfileNameLen)
}

// AddFlag labels f.Schema.f.Table (or its f.Column) with f.Name. Adding a
// flag that already exists with exactly this spelling is a no-op, not an
// error. A row the unique key treats as the same but spelled differently
// is refused with *FlagExistsError rather than silently kept under its
// stored spelling: the key folds case and accents on all four columns, the
// schema, table and column names included (the CLI verb always folded
// those; now it says so instead of reporting an add). Same two-statement
// shape and race note as AddProfile.
func AddFlag(ctx context.Context, db DBExecer, f Flag) error {
	f = f.Trimmed()
	if err := requireFlagKey(f); err != nil {
		return err
	}
	var existing Flag
	err := db.QueryRowContext(ctx, `
		SELECT schema_name, table_name, column_name, flag FROM table_flags
		WHERE schema_name = ? AND table_name = ? AND column_name = ? AND flag = ?`,
		f.Schema, f.Table, f.Column, f.Name).Scan(&existing.Schema, &existing.Table, &existing.Column, &existing.Name)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to look up flag: %w", err)
	}
	if err == nil && existing != f {
		return &FlagExistsError{Existing: existing, Requested: f}
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO table_flags (schema_name, table_name, column_name, flag)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE flag = flag`,
		f.Schema, f.Table, f.Column, f.Name)
	if err != nil {
		return fmt.Errorf("failed to add flag: %w", err)
	}
	return nil
}

// RemoveFlag deletes the one row matching f's key; *FlagNotFoundError when
// there was none.
func RemoveFlag(ctx context.Context, db DBExecer, f Flag) error {
	f = f.Trimmed()
	if err := requireFlagKey(f); err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `
		DELETE FROM table_flags
		WHERE schema_name = ? AND table_name = ? AND column_name = ? AND flag = ?`,
		f.Schema, f.Table, f.Column, f.Name)
	if err != nil {
		return fmt.Errorf("failed to remove flag: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &FlagNotFoundError{Flag: f}
	}
	return nil
}

// ListFlags returns every flag, optionally narrowed to a schema and a table,
// ordered by schema, table, column, flag.
func ListFlags(ctx context.Context, db DBExecer, schema, table string) ([]Flag, error) {
	schema, table = strings.TrimSpace(schema), strings.TrimSpace(table)
	q := `SELECT schema_name, table_name, column_name, flag, created_at FROM table_flags`
	var where []string
	var args []any
	if schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, schema)
	}
	if table != "" {
		where = append(where, "table_name = ?")
		args = append(args, table)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY schema_name, table_name, column_name, flag"
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list flags: %w", err)
	}
	defer rows.Close()
	var out []Flag
	for rows.Next() {
		var f Flag
		if err := rows.Scan(&f.Schema, &f.Table, &f.Column, &f.Name, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return out, nil
}

// AddProfile creates p, or updates the description of an existing profile
// of exactly that name. A name the index's unique key would treat as the
// same profile but spelled differently (letter case, for one) is refused
// with *ProfileExistsError rather than silently re-describing the existing
// row: the caller asked for a new profile and would otherwise be told one
// was added. The lookup and the insert are two statements, so two callers
// racing on the same name can still both land on the one row; the unique
// key keeps that a re-description, never a duplicate.
func AddProfile(ctx context.Context, db DBExecer, p Profile) error {
	p = p.Trimmed()
	if err := requireProfileName(p.Name); err != nil {
		return err
	}
	if n := len(p.Description); n > MaxDescriptionLen {
		return &TooLongError{Field: "description", Got: n, Max: MaxDescriptionLen, Unit: "bytes"}
	}
	// The comparison runs under the column's own collation, which is what
	// the unique key uses, so this finds exactly the row the INSERT would
	// collide with.
	var existing string
	err := db.QueryRowContext(ctx, `SELECT name FROM profiles WHERE name = ?`, p.Name).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to look up profile: %w", err)
	}
	if err == nil && existing != p.Name {
		return &ProfileExistsError{Existing: existing, Requested: p.Name}
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO profiles (name, description)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE description = VALUES(description)`,
		p.Name, p.Description)
	if err != nil {
		return fmt.Errorf("failed to add profile: %w", err)
	}
	return nil
}

// RemoveProfile deletes the profile; its access rules go with it (the
// foreign key cascades). *ProfileNotFoundError when there was none.
func RemoveProfile(ctx context.Context, db DBExecer, name string) error {
	name = strings.TrimSpace(name)
	if err := requireProfileName(name); err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `DELETE FROM profiles WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("failed to remove profile: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &ProfileNotFoundError{Name: name}
	}
	return nil
}

// ListProfiles returns every profile ordered by name.
func ListProfiles(ctx context.Context, db DBExecer) ([]Profile, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT name, COALESCE(description, ''), created_at
		FROM profiles
		ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("failed to list profiles: %w", err)
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var p Profile
		if err := rows.Scan(&p.Name, &p.Description, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return out, nil
}

// ValidateRule is the input check AddRule runs, exposed so a caller can
// refuse bad input before opening a connection. It checks r as given; the
// writers trim first (AddRule calls it on r.Trimmed()), so a caller that
// wants the trimmed verdict passes r.Trimmed() itself.
func ValidateRule(r Rule) error {
	// Permission first. On the command line cobra enforces that --profile,
	// --flag and --permission are PRESENT, not what --permission says, so a
	// bad permission is the one refusal the CLI reaches here and the one it
	// has always given first; keeping it ahead of the field checks keeps
	// that byte-identical.
	if r.Permission != PermissionAllow && r.Permission != PermissionDeny {
		return &InvalidPermissionError{Got: r.Permission}
	}
	if r.Profile == "" {
		return &MissingFieldError{Field: "profile"}
	}
	if r.Flag == "" {
		return &MissingFieldError{Field: "flag"}
	}
	if err := checkLen("profile", r.Profile, MaxProfileNameLen); err != nil {
		return err
	}
	return checkLen("flag", r.Flag, MaxFlagLen)
}

// AddRule maps r.Profile to r.Flag with r.Permission, replacing the
// permission of an existing rule for that pair. The profile must exist
// (*ProfileNotFoundError); the flag name is free text, so a rule may name a
// flag no table carries yet.
func AddRule(ctx context.Context, db DBExecer, r Rule) error {
	r = r.Trimmed()
	if err := ValidateRule(r); err != nil {
		return err
	}
	var profileID int64
	err := db.QueryRowContext(ctx, `SELECT id FROM profiles WHERE name = ?`, r.Profile).Scan(&profileID)
	if errors.Is(err, sql.ErrNoRows) {
		return &ProfileNotFoundError{Name: r.Profile}
	}
	if err != nil {
		return fmt.Errorf("failed to look up profile: %w", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO access_rules (profile_id, flag, permission)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE permission = VALUES(permission)`,
		profileID, r.Flag, r.Permission)
	if err != nil {
		return fmt.Errorf("failed to add access rule: %w", err)
	}
	return nil
}

// RemoveRule deletes the rule for (profile, flag); *RuleNotFoundError when
// there was none.
func RemoveRule(ctx context.Context, db DBExecer, profile, flag string) error {
	profile, flag = strings.TrimSpace(profile), strings.TrimSpace(flag)
	if profile == "" {
		return &MissingFieldError{Field: "profile"}
	}
	if flag == "" {
		return &MissingFieldError{Field: "flag"}
	}
	if err := checkLen("profile", profile, MaxProfileNameLen); err != nil {
		return err
	}
	if err := checkLen("flag", flag, MaxFlagLen); err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `
		DELETE ar FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		WHERE p.name = ? AND ar.flag = ?`,
		profile, flag)
	if err != nil {
		return fmt.Errorf("failed to remove access rule: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &RuleNotFoundError{Profile: profile, Flag: flag}
	}
	return nil
}

// ListRules returns every rule, optionally narrowed to one profile, ordered
// by profile then flag.
func ListRules(ctx context.Context, db DBExecer, profile string) ([]Rule, error) {
	profile = strings.TrimSpace(profile)
	q := `SELECT p.name, ar.flag, ar.permission, ar.created_at
	      FROM access_rules ar
	      JOIN profiles p ON ar.profile_id = p.id`
	var args []any
	if profile != "" {
		q += " WHERE p.name = ?"
		args = append(args, profile)
	}
	q += " ORDER BY p.name, ar.flag"
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list access rules: %w", err)
	}
	defer rows.Close()
	var out []Rule
	for rows.Next() {
		var r Rule
		if err := rows.Scan(&r.Profile, &r.Flag, &r.Permission, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return out, nil
}
