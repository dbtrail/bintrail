package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Store defines the persistence interface for OAuth and tenant data.
type Store interface {
	// OAuth clients (DCR).
	CreateClient(ctx context.Context, client *OAuthClient) error
	GetClient(ctx context.Context, clientID string) (*OAuthClient, error)

	// Authorization codes.
	CreateCode(ctx context.Context, code *AuthCode) error
	ConsumeCode(ctx context.Context, code string) (*AuthCode, error)

	// Access tokens.
	CreateToken(ctx context.Context, token *TokenRecord) error
	ValidateToken(ctx context.Context, accessToken string) (*TokenRecord, error)
	RevokeToken(ctx context.Context, accessTokenHash string) error

	// Refresh tokens.
	CreateRefreshToken(ctx context.Context, rt *RefreshTokenRecord) error
	ConsumeRefreshToken(ctx context.Context, refreshToken string) (*RefreshTokenRecord, error)

	// Tenant lookup.
	GetTenant(ctx context.Context, tenantID string) (*Tenant, error)
}

// OAuthClient represents a dynamically registered OAuth client.
type OAuthClient struct {
	ClientID     string   `dynamodbav:"client_id"`
	ClientSecret string   `dynamodbav:"client_secret"`
	ClientName   string   `dynamodbav:"client_name"`
	RedirectURIs []string `dynamodbav:"redirect_uris"`
	GrantTypes   []string `dynamodbav:"grant_types"`
	CreatedAt    int64    `dynamodbav:"created_at"`
}

// AuthCode represents a short-lived authorization code.
type AuthCode struct {
	Code          string `dynamodbav:"code"`
	ClientID      string `dynamodbav:"client_id"`
	TenantID      string `dynamodbav:"tenant_id"`
	RedirectURI   string `dynamodbav:"redirect_uri"`
	CodeChallenge string `dynamodbav:"code_challenge"`
	State         string `dynamodbav:"state"`
	ExpiresAt     int64  `dynamodbav:"expires_at"` // DynamoDB TTL
}

// TokenRecord represents an active access token.
type TokenRecord struct {
	AccessTokenHash string `dynamodbav:"access_token_hash"`
	ClientID        string `dynamodbav:"client_id"`
	TenantID        string `dynamodbav:"tenant_id"`
	ExpiresAt       int64  `dynamodbav:"expires_at"` // DynamoDB TTL
}

// RefreshTokenRecord represents a refresh token.
type RefreshTokenRecord struct {
	RefreshTokenHash string `dynamodbav:"refresh_token_hash"`
	ClientID         string `dynamodbav:"client_id"`
	TenantID         string `dynamodbav:"tenant_id"`
	ExpiresAt        int64  `dynamodbav:"expires_at"` // DynamoDB TTL
}

// Tenant represents a customer's backend mapping.
type Tenant struct {
	TenantID   string `dynamodbav:"tenant_id"`
	Tier       string `dynamodbav:"tier"`
	BackendURL string `dynamodbav:"backend_url"`
	IndexDSN   string `dynamodbav:"index_dsn"`
	Status     string `dynamodbav:"status"`
}

// ─── DynamoDB implementation ─────────────────────────────────────────────────

// DynamoStore implements Store using DynamoDB tables.
type DynamoStore struct {
	client *dynamodb.Client
	tables struct {
		clients  string
		codes    string
		tokens   string
		refresh  string
		tenants  string
	}
}

// NewDynamoStore creates a DynamoStore using default AWS credentials.
func NewDynamoStore(tablePrefix string) (*DynamoStore, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	s := &DynamoStore{
		client: dynamodb.NewFromConfig(cfg),
	}
	s.tables.clients = tablePrefix + "-clients"
	s.tables.codes = tablePrefix + "-codes"
	s.tables.tokens = tablePrefix + "-tokens"
	s.tables.refresh = tablePrefix + "-refresh"
	s.tables.tenants = tablePrefix + "-tenants"
	return s, nil
}

func (s *DynamoStore) CreateClient(ctx context.Context, client *OAuthClient) error {
	item, err := attributevalue.MarshalMap(client)
	if err != nil {
		return fmt.Errorf("marshal client: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tables.clients,
		Item:      item,
	})
	return err
}

func (s *DynamoStore) GetClient(ctx context.Context, clientID string) (*OAuthClient, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tables.clients,
		Key: map[string]types.AttributeValue{
			"client_id": &types.AttributeValueMemberS{Value: clientID},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	var c OAuthClient
	if err := attributevalue.UnmarshalMap(out.Item, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *DynamoStore) CreateCode(ctx context.Context, code *AuthCode) error {
	item, err := attributevalue.MarshalMap(code)
	if err != nil {
		return fmt.Errorf("marshal code: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tables.codes,
		Item:      item,
	})
	return err
}

func (s *DynamoStore) ConsumeCode(ctx context.Context, code string) (*AuthCode, error) {
	// Get and delete atomically to prevent replay.
	out, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tables.codes,
		Key: map[string]types.AttributeValue{
			"code": &types.AttributeValueMemberS{Value: code},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return nil, err
	}
	if out.Attributes == nil {
		return nil, nil
	}
	var ac AuthCode
	if err := attributevalue.UnmarshalMap(out.Attributes, &ac); err != nil {
		return nil, err
	}
	if ac.ExpiresAt < time.Now().Unix() {
		return nil, nil // expired
	}
	return &ac, nil
}

func (s *DynamoStore) CreateToken(ctx context.Context, token *TokenRecord) error {
	item, err := attributevalue.MarshalMap(token)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tables.tokens,
		Item:      item,
	})
	return err
}

func (s *DynamoStore) ValidateToken(ctx context.Context, accessToken string) (*TokenRecord, error) {
	hash := HashToken(accessToken)
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tables.tokens,
		Key: map[string]types.AttributeValue{
			"access_token_hash": &types.AttributeValueMemberS{Value: hash},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	var t TokenRecord
	if err := attributevalue.UnmarshalMap(out.Item, &t); err != nil {
		return nil, err
	}
	if t.ExpiresAt < time.Now().Unix() {
		return nil, nil // expired
	}
	return &t, nil
}

func (s *DynamoStore) RevokeToken(ctx context.Context, accessTokenHash string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tables.tokens,
		Key: map[string]types.AttributeValue{
			"access_token_hash": &types.AttributeValueMemberS{Value: accessTokenHash},
		},
	})
	return err
}

func (s *DynamoStore) CreateRefreshToken(ctx context.Context, rt *RefreshTokenRecord) error {
	item, err := attributevalue.MarshalMap(rt)
	if err != nil {
		return fmt.Errorf("marshal refresh token: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tables.refresh,
		Item:      item,
	})
	return err
}

func (s *DynamoStore) ConsumeRefreshToken(ctx context.Context, refreshToken string) (*RefreshTokenRecord, error) {
	hash := HashToken(refreshToken)
	// Delete and return old value — single-use refresh tokens.
	out, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tables.refresh,
		Key: map[string]types.AttributeValue{
			"refresh_token_hash": &types.AttributeValueMemberS{Value: hash},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return nil, err
	}
	if out.Attributes == nil {
		return nil, nil
	}
	var rt RefreshTokenRecord
	if err := attributevalue.UnmarshalMap(out.Attributes, &rt); err != nil {
		return nil, err
	}
	if rt.ExpiresAt < time.Now().Unix() {
		return nil, nil
	}
	return &rt, nil
}

func (s *DynamoStore) GetTenant(ctx context.Context, tenantID string) (*Tenant, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tables.tenants,
		Key: map[string]types.AttributeValue{
			"tenant_id": &types.AttributeValueMemberS{Value: tenantID},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	var t Tenant
	if err := attributevalue.UnmarshalMap(out.Item, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// HashToken returns the SHA-256 hex hash of a token string.
func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// GenerateToken generates a cryptographically random token as a hex string.
func GenerateToken(nbytes int) (string, error) {
	b := make([]byte, nbytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ─── In-memory store for testing ─────────────────────────────────────────────

// Compile-time check that MemoryStore implements Store.
var _ Store = (*MemoryStore)(nil)
var _ Store = (*DynamoStore)(nil)

// MemoryStore is an in-memory Store implementation for testing.
type MemoryStore struct {
	Clients       map[string]*OAuthClient
	Codes         map[string]*AuthCode
	Tokens        map[string]*TokenRecord        // keyed by access_token_hash
	RefreshTokens map[string]*RefreshTokenRecord  // keyed by refresh_token_hash
	Tenants       map[string]*Tenant
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		Clients:       make(map[string]*OAuthClient),
		Codes:         make(map[string]*AuthCode),
		Tokens:        make(map[string]*TokenRecord),
		RefreshTokens: make(map[string]*RefreshTokenRecord),
		Tenants:       make(map[string]*Tenant),
	}
}

func (m *MemoryStore) CreateClient(_ context.Context, c *OAuthClient) error {
	m.Clients[c.ClientID] = c
	return nil
}

func (m *MemoryStore) GetClient(_ context.Context, clientID string) (*OAuthClient, error) {
	return m.Clients[clientID], nil
}

func (m *MemoryStore) CreateCode(_ context.Context, code *AuthCode) error {
	m.Codes[code.Code] = code
	return nil
}

func (m *MemoryStore) ConsumeCode(_ context.Context, code string) (*AuthCode, error) {
	ac, ok := m.Codes[code]
	if !ok {
		return nil, nil
	}
	delete(m.Codes, code)
	if ac.ExpiresAt < time.Now().Unix() {
		return nil, nil
	}
	return ac, nil
}

func (m *MemoryStore) CreateToken(_ context.Context, t *TokenRecord) error {
	m.Tokens[t.AccessTokenHash] = t
	return nil
}

func (m *MemoryStore) ValidateToken(_ context.Context, accessToken string) (*TokenRecord, error) {
	hash := HashToken(accessToken)
	t, ok := m.Tokens[hash]
	if !ok {
		return nil, nil
	}
	if t.ExpiresAt < time.Now().Unix() {
		return nil, nil
	}
	return t, nil
}

func (m *MemoryStore) RevokeToken(_ context.Context, accessTokenHash string) error {
	delete(m.Tokens, accessTokenHash)
	return nil
}

func (m *MemoryStore) CreateRefreshToken(_ context.Context, rt *RefreshTokenRecord) error {
	m.RefreshTokens[rt.RefreshTokenHash] = rt
	return nil
}

func (m *MemoryStore) ConsumeRefreshToken(_ context.Context, refreshToken string) (*RefreshTokenRecord, error) {
	hash := HashToken(refreshToken)
	rt, ok := m.RefreshTokens[hash]
	if !ok {
		return nil, nil
	}
	delete(m.RefreshTokens, hash)
	if rt.ExpiresAt < time.Now().Unix() {
		return nil, nil
	}
	return rt, nil
}

func (m *MemoryStore) GetTenant(_ context.Context, tenantID string) (*Tenant, error) {
	return m.Tenants[tenantID], nil
}
