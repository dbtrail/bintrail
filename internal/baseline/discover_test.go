package baseline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverTables_EmptyTable(t *testing.T) {
	dir := t.TempDir()

	// Normal table: schema + data file.
	writeFile(t, dir, "shop.orders-schema.sql",
		"CREATE TABLE `orders` (\n  `id` int,\n  `total` decimal(10,2)\n) ENGINE=InnoDB;\n")
	writeFile(t, dir, "shop.orders.00000.sql",
		"INSERT INTO `orders` VALUES (1, 99.99);\n")

	// Empty table: schema only, no data file.
	writeFile(t, dir, "shop.empty_claims-schema.sql",
		"CREATE TABLE `empty_claims` (\n  `id` int,\n  `status` varchar(50)\n) ENGINE=InnoDB;\n")

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}

	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2", len(tables))
	}

	// Results are sorted alphabetically: empty_claims, orders.
	if tables[0].Table != "empty_claims" {
		t.Errorf("tables[0].Table = %q, want %q", tables[0].Table, "empty_claims")
	}
	if len(tables[0].DataFiles) != 0 {
		t.Errorf("empty table DataFiles = %d, want 0", len(tables[0].DataFiles))
	}
	if tables[0].Format != "sql" {
		t.Errorf("empty table Format = %q, want %q", tables[0].Format, "sql")
	}

	if tables[1].Table != "orders" {
		t.Errorf("tables[1].Table = %q, want %q", tables[1].Table, "orders")
	}
	if len(tables[1].DataFiles) != 1 {
		t.Errorf("orders DataFiles = %d, want 1", len(tables[1].DataFiles))
	}
}

func TestDiscoverTables_ViewSkipped(t *testing.T) {
	dir := t.TempDir()

	// View: schema only, CREATE VIEW statement.
	writeFile(t, dir, "shop.my_view-schema.sql",
		"CREATE VIEW `my_view` AS SELECT * FROM orders;\n")

	// Normal table with data.
	writeFile(t, dir, "shop.orders-schema.sql",
		"CREATE TABLE `orders` (\n  `id` int\n) ENGINE=InnoDB;\n")
	writeFile(t, dir, "shop.orders.00000.sql",
		"INSERT INTO `orders` VALUES (1);\n")

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("got %d tables, want 1 (view should be skipped)", len(tables))
	}
	if tables[0].Table != "orders" {
		t.Errorf("tables[0].Table = %q, want %q", tables[0].Table, "orders")
	}
}

func TestDiscoverTables_ViewWithAlgorithm(t *testing.T) {
	dir := t.TempDir()

	// View with ALGORITHM=UNDEFINED DEFINER=... pattern (common in MySQL 8.0 dumps).
	writeFile(t, dir, "shop.fancy_view-schema.sql",
		"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `fancy_view` AS SELECT 1;\n")

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}

	if len(tables) != 0 {
		t.Fatalf("got %d tables, want 0 (view with ALGORITHM should be skipped)", len(tables))
	}
}

func TestIsView(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{
			name:    "create table",
			content: "CREATE TABLE `t` (\n  `id` int\n) ENGINE=InnoDB;\n",
			want:    false,
		},
		{
			name:    "create view",
			content: "CREATE VIEW `v` AS SELECT 1;\n",
			want:    true,
		},
		{
			name:    "create view with algorithm",
			content: "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT 1;\n",
			want:    true,
		},
		{
			name:    "empty file",
			content: "",
			want:    false, // assume table — 0-row Parquet is harmless
		},
		{
			name:    "comment before create table",
			content: "-- MySQL dump\nCREATE TABLE `t` (`id` int);\n",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "schema.sql")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if got := isView(path); got != tt.want {
				t.Errorf("isView() = %v, want %v", got, tt.want)
			}
		})
	}

	// Non-existent file → false (assume table — 0-row Parquet is harmless).
	t.Run("non-existent file", func(t *testing.T) {
		if got := isView("/no/such/file.sql"); got != false {
			t.Errorf("isView(missing) = %v, want false", got)
		}
	})
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
