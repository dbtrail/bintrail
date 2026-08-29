package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Environment variables that point every S3 path (SDK clients and DuckDB
// httpfs reads alike) at an S3-compatible store instead of AWS (#1453/#1454).
const (
	// EnvS3Endpoint is the store's URL: scheme://host[:port], no path.
	// Examples: http://minio:9000, https://s3.wasabisys.com. Empty falls back
	// to the AWS SDK's own endpoint variables below, and then to AWS S3.
	// This is the variable bintrail owns, and the only one it validates and
	// applies its own addressing rules to.
	EnvS3Endpoint = "BINTRAIL_S3_ENDPOINT"
	// EnvS3PathStyle forces bucket-in-path addressing (http://host/bucket/key)
	// on or off. Default with EnvS3Endpoint: ON, which is what MinIO and
	// LocalStack need; a virtual-hosted-only store sets it to 0. Set
	// explicitly, it also applies to an endpoint the AWS SDK resolved on its
	// own (its env vars, or endpoint_url in ~/.aws/config).
	EnvS3PathStyle = "BINTRAIL_S3_PATH_STYLE"
)

// awsEndpointEnv are the AWS SDK's OWN endpoint variables. bintrail reads them
// so the DuckDB half follows an operator who already configured the SDK half:
// httpfs does not look at them (measured against DuckDB v1.5.5 — with
// AWS_ENDPOINT_URL_S3 set and nothing else configured, a read still goes to
// s3.amazonaws.com). It does not police them, though: the SDK accepts
// values this package cannot parse, and failing every command over one would
// break a working setup on upgrade. They therefore never carry bintrail's
// validation, its path-style default, or its region handling.
var awsEndpointEnv = []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"}

// warnUnparseableOnce keeps the "cannot mirror this to DuckDB" warning to one
// line per process; the condition is a static environment value.
var warnUnparseableOnce sync.Once

// ErrS3EndpointConfig marks a bad BINTRAIL_S3_ENDPOINT / BINTRAIL_S3_PATH_STYLE
// value. It is a configuration fault, knowable before any byte moves, so a
// caller that degrades on read failures (integrity validation) can tell it
// apart from a storage problem and refuse to degrade.
var ErrS3EndpointConfig = errors.New("S3 endpoint configuration")

// S3Endpoint is where S3 requests go. The zero value means AWS S3 with the
// SDK's own addressing; a non-empty URL means an S3-compatible store.
type S3Endpoint struct {
	URL       string // scheme://host[:port]
	PathStyle bool   // bucket in the path rather than in the host name
	// Source is the environment variable URL came from, "" when unset.
	// Managed() reads it: bintrail applies its own rules only to its own
	// variable.
	Source string
	// pathStyleExplicit records that BINTRAIL_S3_PATH_STYLE was set, so the
	// operator's choice applies even to an endpoint the SDK resolved on its
	// own, where bintrail otherwise defers.
	pathStyleExplicit bool
}

// PathStyleExplicit reports whether BINTRAIL_S3_PATH_STYLE named the
// addressing mode, rather than it coming from a default.
func (e S3Endpoint) PathStyleExplicit() bool { return e.pathStyleExplicit }

// Set reports whether an endpoint is configured.
func (e S3Endpoint) Set() bool { return e.URL != "" }

// Managed reports whether the endpoint came from BINTRAIL_S3_ENDPOINT, the
// variable bintrail owns end to end. An endpoint from the SDK's own variables
// is mirrored to DuckDB but otherwise left to the SDK.
func (e S3Endpoint) Managed() bool { return e.Source == EnvS3Endpoint }

// Host returns host[:port] without the scheme, the form DuckDB takes as
// ENDPOINT / s3_endpoint. Empty when no endpoint is set.
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

// URLStyle renders the addressing mode as DuckDB names it.
func (e S3Endpoint) URLStyle() string {
	if e.PathStyle {
		return "path"
	}
	return "vhost"
}

// S3EndpointFromEnv resolves the endpoint from the environment.
//
// A bad BINTRAIL_S3_ENDPOINT or BINTRAIL_S3_PATH_STYLE is an ERROR, never a
// silent fallback to AWS: with writes going to the configured store and reads
// going to an AWS bucket of the same name, a baseline that exists reads as
// missing. An AWS SDK variable that this package cannot parse is NOT an error
// (the SDK owns it, and it may well be valid there); it is logged and left
// out of the DuckDB mirror, which is the most that can be done honestly.
func S3EndpointFromEnv() (S3Endpoint, error) {
	var ep S3Endpoint

	if raw := strings.TrimSpace(os.Getenv(EnvS3Endpoint)); raw != "" {
		normalized, err := normalizeEndpointURL(raw)
		if err != nil {
			// The value is not echoed: one of the shapes this rejects is a URL
			// carrying credentials, and the error reaches CLI stderr, a console
			// 502 body and the logs.
			return S3Endpoint{}, fmt.Errorf("%w: %s: %w", ErrS3EndpointConfig, EnvS3Endpoint, err)
		}
		ep = S3Endpoint{URL: normalized, PathStyle: true, Source: EnvS3Endpoint}
	} else {
		for _, name := range awsEndpointEnv {
			raw := strings.TrimSpace(os.Getenv(name))
			if raw == "" {
				continue
			}
			normalized, err := normalizeEndpointURL(raw)
			if err != nil {
				// The SDK keeps using it; only the DuckDB mirror is lost, and
				// that divergence is exactly what the operator needs told.
				// Once per process, like its twin in s3url.go: the same
				// misconfiguration, the same remedy, and S3EndpointFromEnv
				// runs three times per client construction.
				warnUnparseableOnce.Do(func() {
					slog.Warn("S3 endpoint from the AWS SDK's environment cannot be used for DuckDB reads, which will go to AWS; set "+EnvS3Endpoint+" so both halves agree",
						"variable", name, "error", err)
				})
				// break, not continue: the SDK's own precedence puts
				// AWS_ENDPOINT_URL_S3 above AWS_ENDPOINT_URL, so falling
				// through to the generic one would point DuckDB at a store
				// the SDK is not using — the split this all exists to avoid.
				break
			}
			// PathStyle stays false: the SDK addresses virtual-hosted by
			// default, and the DuckDB half must match what the SDK does.
			ep = S3Endpoint{URL: normalized, Source: name}
			break
		}
	}

	if v := strings.TrimSpace(os.Getenv(EnvS3PathStyle)); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return S3Endpoint{}, fmt.Errorf("%w: %s=%q: want true or false", ErrS3EndpointConfig, EnvS3PathStyle, v) //nolint:err113 // a bool value carries no secret
		}
		ep.PathStyle = b
		ep.pathStyleExplicit = true
	}
	return ep, nil
}

// normalizeEndpointURL accepts scheme://host[:port] and returns it without a
// trailing slash. Anything else is rejected: a path, query or credentials in
// the URL are shapes bintrail cannot render into DuckDB's ENDPOINT.
func normalizeEndpointURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errors.New("want scheme://host[:port] with scheme http or https")
	}
	if strings.Trim(u.Path, "/") != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", errors.New("only scheme://host[:port] is supported (no path, query or credentials)")
	}
	return u.Scheme + "://" + u.Host, nil
}
