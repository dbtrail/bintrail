package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

// postNDJSON delivers a spooled batch.
//
// This client is deliberately minimal and deliberately NOT internal/byos's
// MetadataClient: that one attaches `Authorization: Bearer <apiKey>` and posts
// to the authenticated data plane. An account-linked credential on the
// telemetry wire would make the metadata-only claim false and would let usage
// data be joined to a customer.
//
// No Authorization header, no cookie, no account identifier — ever. The
// no-credential CI test asserts exactly that against this function.
func postNDJSON(ctx context.Context, client *http.Client, endpoint string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build telemetry request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send telemetry: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("telemetry endpoint returned %s", resp.Status)
	}
	return nil
}
