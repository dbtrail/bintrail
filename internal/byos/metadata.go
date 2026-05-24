package byos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// MetadataClient sends metadata records to the dbtrail API.
type MetadataClient struct {
	endpoint   string // base URL, e.g. "https://api.dbtrail.io"
	apiKey     string
	serverUUID string // pre-registered server UUID (empty for legacy back-compat)
	http       *http.Client
}

// NewMetadataClient creates a client that sends metadata to the given
// dbtrail API endpoint. The endpoint should be a base URL without a
// trailing slash (e.g. "https://api.dbtrail.io"). The apiKey is sent
// as a Bearer token in the Authorization header.
//
// serverUUID mirrors the value the agent's WebSocket channel already
// sends in X-Bintrail-Server-UUID on dial (see internal/agent/channel.go,
// issue #317). When non-empty it is also sent in the same header on
// every POST /v1/events, so the dbtrail backend can resolve a
// dashboard-pre-registered row by UUID instead of creating a duplicate
// byos-<server-id> on the first ingest. Empty preserves legacy behavior.
// See issue #341.
func NewMetadataClient(endpoint, apiKey, serverUUID string) *MetadataClient {
	return &MetadataClient{
		endpoint:   endpoint,
		apiKey:     apiKey,
		serverUUID: serverUUID,
		http:       &http.Client{Timeout: 30 * time.Second},
	}
}

// Send posts a batch of metadata records to the dbtrail API.
// Records are serialized as a JSON array in the request body.
func (c *MetadataClient) Send(ctx context.Context, records []MetadataRecord) error {
	body, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	url := c.endpoint + "/v1/events"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	// Mirror the WS-handshake header so the SaaS resolves pre-registered
	// rows by UUID on first ingest (#341). Omit entirely when empty so an
	// older backend isn't confused by an empty value, matching the WS dial
	// pattern in internal/agent/channel.go.
	if c.serverUUID != "" {
		req.Header.Set("X-Bintrail-Server-UUID", c.serverUUID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("send metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("metadata API returned %s: %s", resp.Status, detail)
	}
	return nil
}
