package icebergexport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

// catalogName is the filesystem catalog's name; it only labels the warehouse.
const catalogName = "bintrail"

// lockFileName is the single-writer lock at the warehouse root. The
// filesystem catalog commits by writing a new metadata file and then a
// version hint; two writers racing that would each believe they won. bintrail
// is the single writer of these tables by design, and the lock makes an
// accidental second run refuse instead of clobber.
const lockFileName = ".bintrail-export.lock"

// openWarehouse creates the warehouse directory if needed, takes the
// single-writer lock and opens the filesystem catalog over it. The returned
// release must be called when the run ends.
func openWarehouse(ctx context.Context, dir string) (*hadoop.Catalog, func(), error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create warehouse %s: %w", abs, err)
	}
	release, err := lockWarehouse(filepath.Join(abs, lockFileName))
	if err != nil {
		return nil, nil, err
	}
	cat, err := hadoop.NewCatalog(catalogName, abs, nil)
	if err != nil {
		release()
		return nil, nil, fmt.Errorf("open warehouse %s: %w", abs, err)
	}
	return cat, release, nil
}

// ensureNamespace creates the schema's namespace when it does not exist.
func ensureNamespace(ctx context.Context, cat *hadoop.Catalog, schema string) error {
	ns := catalog.ToIdentifier(schema)
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	if err != nil {
		return fmt.Errorf("check namespace %s: %w", schema, err)
	}
	if exists {
		return nil
	}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return fmt.Errorf("create namespace %s: %w", schema, err)
	}
	return nil
}

// loadTable returns the table when it exists, or (nil, false, nil) when the
// catalog has no such table.
func loadTable(ctx context.Context, cat *hadoop.Catalog, ident table.Identifier) (*table.Table, bool, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("load table %v: %w", ident, err)
	}
	return tbl, true, nil
}

// tableProperties are the properties a new table is created with. Format
// version 2 is what equality deletes require; the merge-on-read modes make an
// engine that later runs DELETE or UPDATE on the table keep the same posture
// instead of rewriting files.
func tableProperties() iceberg.Properties {
	return iceberg.Properties{
		table.PropertyFormatVersion: "2",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		"write.update.mode":         table.WriteModeMergeOnRead,
		"write.merge.mode":          table.WriteModeMergeOnRead,
		propVersion:                 exportVersion,
	}
}
