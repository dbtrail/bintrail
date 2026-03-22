package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/spf13/cobra"
)

var generateKeyCmd = &cobra.Command{
	Use:   "generate-key",
	Short: "Generate an encryption key for at-rest dump encryption",
	Long: `Generates a 32-byte random encryption key, base64-encodes it, and writes it
to a file. This key is used by 'dump --encrypt' to encrypt mydumper output
files and by 'baseline --encrypt' to decrypt them before Parquet conversion.

Default location: ~/.config/bintrail/dump.key
Override with --output.`,
	RunE: runGenerateKey,
}

var (
	gkOutput string
	gkFormat string
)

func init() {
	generateKeyCmd.Flags().StringVar(&gkOutput, "output", "", "Output path for the key file (default: ~/.config/bintrail/dump.key)")
	generateKeyCmd.Flags().StringVar(&gkFormat, "format", "text", "Output format: text or json")

	rootCmd.AddCommand(generateKeyCmd)
}

// defaultKeyPath returns ~/.config/bintrail/dump.key.
func defaultKeyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".config", "bintrail", "dump.key")
	}
	return filepath.Join(home, ".config", "bintrail", "dump.key")
}

func runGenerateKey(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(gkFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", gkFormat)
	}

	keyPath := gkOutput
	if keyPath == "" {
		keyPath = defaultKeyPath()
	}

	// Refuse to overwrite an existing file.
	if _, err := os.Stat(keyPath); err == nil {
		return fmt.Errorf("key file already exists at %s; delete it first to regenerate", keyPath)
	}

	// Generate 32 bytes of cryptographically secure random data.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("generate random key: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(raw)

	// Create parent directory if needed.
	dir := filepath.Dir(keyPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	// Write key file with restricted permissions.
	if err := os.WriteFile(keyPath, []byte(encoded+"\n"), 0o600); err != nil {
		return fmt.Errorf("write key file: %w", err)
	}

	slog.Info("encryption key generated", "path", keyPath)

	if gkFormat == "json" {
		return outputJSON(struct {
			KeyFile string `json:"key_file"`
		}{KeyFile: keyPath})
	}
	fmt.Printf("Encryption key written to %s\n", keyPath)
	return nil
}
