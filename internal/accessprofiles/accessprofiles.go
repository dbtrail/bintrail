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

// IsRefusal reports whether err is one of this package's typed refusals
// (bad input or a missing row) rather than a database failure. Callers use
// it to pick a 4xx over a 500 without listing every type.
func IsRefusal(err error) bool {
	var mf *MissingFieldError
	var ip *InvalidPermissionError
	var pnf *ProfileNotFoundError
	var fnf *FlagNotFoundError
	var rnf *RuleNotFoundError
	return errors.As(err, &mf) || errors.As(err, &ip) || errors.As(err, &pnf) ||
		errors.As(err, &fnf) || errors.As(err, &rnf)
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
	return nil
}

// AddFlag labels f.Schema.f.Table (or its f.Column) with f.Name. Adding a
// flag that already exists is a no-op, not an error.
func AddFlag(ctx context.Context, db DBExecer, f Flag) error {
	if err := requireFlagKey(f); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
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
// of that name.
func AddProfile(ctx context.Context, db DBExecer, p Profile) error {
	if p.Name == "" {
		return &MissingFieldError{Field: "profile name"}
	}
	_, err := db.ExecContext(ctx, `
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
	if name == "" {
		return &MissingFieldError{Field: "profile name"}
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
// refuse bad input before opening a connection.
func ValidateRule(r Rule) error {
	// Permission first: it is the one value the command line does not
	// enforce as a required flag, so it is the refusal the CLI has always
	// given first.
	if r.Permission != PermissionAllow && r.Permission != PermissionDeny {
		return &InvalidPermissionError{Got: r.Permission}
	}
	if r.Profile == "" {
		return &MissingFieldError{Field: "profile"}
	}
	if r.Flag == "" {
		return &MissingFieldError{Field: "flag"}
	}
	return nil
}

// AddRule maps r.Profile to r.Flag with r.Permission, replacing the
// permission of an existing rule for that pair. The profile must exist
// (*ProfileNotFoundError); the flag name is free text, so a rule may name a
// flag no table carries yet.
func AddRule(ctx context.Context, db DBExecer, r Rule) error {
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
	if profile == "" {
		return &MissingFieldError{Field: "profile"}
	}
	if flag == "" {
		return &MissingFieldError{Field: "flag"}
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
