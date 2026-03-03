package main

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_clientLifecycle(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	client := &OAuthClient{
		ClientID:     "test-id",
		ClientSecret: "test-secret",
		ClientName:   "Test",
		RedirectURIs: []string{"https://example.com/callback"},
	}

	if err := store.CreateClient(ctx, client); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := store.GetClient(ctx, "test-id")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected client, got nil")
	}
	if got.ClientSecret != "test-secret" {
		t.Errorf("expected secret test-secret, got %s", got.ClientSecret)
	}

	// Unknown client.
	got, err = store.GetClient(ctx, "unknown")
	if err != nil {
		t.Fatalf("get unknown: %v", err)
	}
	if got != nil {
		t.Error("expected nil for unknown client")
	}
}

func TestMemoryStore_codeConsumeOnce(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	code := &AuthCode{
		Code:      "abc123",
		ClientID:  "client",
		TenantID:  "tenant",
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}
	store.CreateCode(ctx, code)

	// First consume succeeds.
	got, err := store.ConsumeCode(ctx, "abc123")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got == nil {
		t.Fatal("expected code, got nil")
	}
	if got.TenantID != "tenant" {
		t.Errorf("expected tenant, got %s", got.TenantID)
	}

	// Second consume returns nil (code was deleted).
	got, err = store.ConsumeCode(ctx, "abc123")
	if err != nil {
		t.Fatalf("consume again: %v", err)
	}
	if got != nil {
		t.Error("expected nil on second consume")
	}
}

func TestMemoryStore_expiredCode(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	code := &AuthCode{
		Code:      "expired",
		ExpiresAt: time.Now().Add(-1 * time.Minute).Unix(), // already expired
	}
	store.CreateCode(ctx, code)

	got, _ := store.ConsumeCode(ctx, "expired")
	if got != nil {
		t.Error("expected nil for expired code")
	}
}

func TestMemoryStore_tokenValidation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	accessToken := "my-access-token"
	rec := &TokenRecord{
		AccessTokenHash: HashToken(accessToken),
		ClientID:        "client",
		TenantID:        "tenant",
		ExpiresAt:       time.Now().Add(1 * time.Hour).Unix(),
	}
	store.CreateToken(ctx, rec)

	// Valid token.
	got, err := store.ValidateToken(ctx, accessToken)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got == nil {
		t.Fatal("expected token record")
	}
	if got.TenantID != "tenant" {
		t.Errorf("expected tenant, got %s", got.TenantID)
	}

	// Wrong token.
	got, _ = store.ValidateToken(ctx, "wrong-token")
	if got != nil {
		t.Error("expected nil for wrong token")
	}
}

func TestMemoryStore_refreshTokenRotation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	rt := "refresh-token"
	store.CreateRefreshToken(ctx, &RefreshTokenRecord{
		RefreshTokenHash: HashToken(rt),
		ClientID:         "client",
		TenantID:         "tenant",
		ExpiresAt:        time.Now().Add(24 * time.Hour).Unix(),
	})

	// Consume returns the record and deletes it.
	got, err := store.ConsumeRefreshToken(ctx, rt)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got == nil {
		t.Fatal("expected refresh token record")
	}

	// Second consume returns nil.
	got, _ = store.ConsumeRefreshToken(ctx, rt)
	if got != nil {
		t.Error("expected nil on second consume (token rotation)")
	}
}

func TestHashToken(t *testing.T) {
	hash1 := HashToken("token-a")
	hash2 := HashToken("token-b")
	if hash1 == hash2 {
		t.Error("different tokens should have different hashes")
	}
	if len(hash1) != 64 { // SHA-256 hex = 64 chars
		t.Errorf("expected 64-char hash, got %d", len(hash1))
	}
	// Deterministic.
	if HashToken("token-a") != hash1 {
		t.Error("hash should be deterministic")
	}
}

func TestGenerateToken(t *testing.T) {
	token1, err := GenerateToken(32)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(token1) != 64 { // 32 bytes = 64 hex chars
		t.Errorf("expected 64-char token, got %d", len(token1))
	}

	token2, _ := GenerateToken(32)
	if token1 == token2 {
		t.Error("tokens should be unique")
	}
}
