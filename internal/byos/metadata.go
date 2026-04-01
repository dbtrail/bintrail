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
	endpoint string // base URL, e.g. "https://api.dbtrail.io"
	apiKey   string
	http     *http.Client
}

// NewMetadataClient creates a client that sends metadata to the given
// dbtrail API endpoint. The endpoint should be a base URL without a
// trailing slash (e.g. "https://api.dbtrail.io"). The apiKey is sent
// as a Bearer token in the Authorization header.
func NewMetadataClient(endpoint, apiKey string) *MetadataClient {
	return &MetadataClient{
		endpoint: endpoint,
		apiKey:   apiKey,
		http:     &http.Client{Timeout: 30 * time.Second},
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
