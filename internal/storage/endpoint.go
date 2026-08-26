package storage

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Environment variables that point every S3 path (SDK clients and DuckDB
// httpfs reads alike) at an S3-compatible store instead of AWS (#1453/#1454).
const (
	// EnvS3Endpoint is the store's URL: scheme://host[:port], no path.
	// Examples: http://minio:9000, https://s3.wasabisys.com. Empty = AWS S3.
	// The AWS SDK's own AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL are honored as
	// fallbacks so an operator who already set those gets the same routing on
	// the DuckDB half, which the SDK variables would otherwise never reach.
	EnvS3Endpoint = "BINTRAIL_S3_ENDPOINT"
	// EnvS3PathStyle forces bucket-in-path addressing (http://host/bucket/key)
	// on or off. Default when an endpoint is set: ON, which is what MinIO and
	// LocalStack need; a virtual-hosted-only store sets it to 0.
	EnvS3PathStyle = "BINTRAIL_S3_PATH_STYLE"
)

// S3Endpoint is where S3 requests go. The zero value means AWS S3 with the
// SDK's own addressing; a non-empty URL means an S3-compatible store.
type S3Endpoint struct {
	URL       string // scheme://host[:port], validated by S3EndpointFromEnv
	PathStyle bool   // bucket in the path rather than in the host name
}

// Set reports whether a custom endpoint is configured.
func (e S3Endpoint) Set() bool { return e.URL != "" }

// Host returns host[:port] without the scheme, the form DuckDB's httpfs
// secret takes as ENDPOINT. Empty when no endpoint is set.
func (e S3Endpoint) Host() string {
	if !e.Set() {
		return ""
	}
	u, err := url.Parse(e.URL)
	if err != nil {
		return ""
	}
	return u.Host
}

// UseSSL reports whether the endpoint is https. False for the zero value too,
// which callers must not read as "plain http to AWS": check Set() first.
func (e S3Endpoint) UseSSL() bool {
	return strings.HasPrefix(strings.ToLower(e.URL), "https://")
}

// S3EndpointFromEnv resolves the endpoint from the environment. An invalid
// value is an ERROR, never a silent fallback to AWS: with the SDK half pointed
// at a custom store, a DuckDB read that quietly went to an AWS bucket of the
// same name is the failure shape #1454 exists to close.
func S3EndpointFromEnv() (S3Endpoint, error) {
	raw, source := "", ""
	for _, name := range []string{EnvS3Endpoint, "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"} {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			raw, source = v, name
			break
		}
	}
	var ep S3Endpoint
	if raw != "" {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return S3Endpoint{}, fmt.Errorf("%s=%q: want scheme://host[:port] with scheme http or https", source, raw)
		}
		if strings.Trim(u.Path, "/") != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return S3Endpoint{}, fmt.Errorf("%s=%q: only scheme://host[:port] is supported (no path, query or credentials)", source, raw)
		}
		ep.URL = u.Scheme + "://" + u.Host
		ep.PathStyle = true
	}
	if v := strings.TrimSpace(os.Getenv(EnvS3PathStyle)); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return S3Endpoint{}, fmt.Errorf("%s=%q: want true or false", EnvS3PathStyle, v)
		}
		ep.PathStyle = b
	}
	return ep, nil
}
