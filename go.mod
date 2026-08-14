module github.com/dbtrail/dbtrail

go 1.25.11

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aws/aws-sdk-go-v2 v1.41.2
	github.com/aws/aws-sdk-go-v2/config v1.32.10
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.18
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.21.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.96.2
	github.com/aws/smithy-go v1.24.1
	github.com/coder/websocket v1.8.14
	// PINNED to the DuckDB 1.4 "Andium" LTS line. v2.5.6 == DuckDB 1.4.5.
	// Patch bumps WITHIN the line (v2.5.7, ...) are expected and welcome — they
	// are where LTS fixes land. What is forbidden is any v2.1xxxx.x, which is
	// DuckDB >= 1.5: duckdb-go changed versioning at 1.5 and now encodes the
	// engine version in the minor component (DuckDB 1.5.5 => v2.10505.0), so
	// under semver v2.10505.0 > v2.5.6 and a plain `go get -u` (or Dependabot,
	// if it is ever enabled here) walks the build off LTS with no import-path
	// change to notice. The six duckdb-go-bindings/lib/* modules use the same
	// encoding and move with this one.
	//
	// This comment cannot enforce itself — `go get -u` rewrites the version and
	// leaves the text byte-identical. The guard that actually fails is
	// TestDuckDBEngineOnLTSLine in internal/duckdbutil; it also carries the
	// pre-flight checklist for the day we do move off.
	//
	// Nothing in 1.5 is reachable from here. Our surface is read-side only:
	// parquet_scan (hive_partitioning/union_by_name), glob, parquet metadata
	// functions, one COPY TO PARQUET, INSTALL/LOAD httpfs+aws, CREATE SECRET
	// credential_chain, and SET threads/memory_limit/temp_directory/s3_region/
	// home_directory. Every handle is in-memory (sql.Open("duckdb", "")) and our
	// durable format is Parquet written by parquet-go, not DuckDB, so 1.5's
	// storage and type headliners cannot reach us. What 1.5 DOES change is
	// httpfs's backend, httplib -> curl, landing on the S3-direct scan (#511),
	// credential_chain secrets, cross-region resolution and the $HOME/.duckdb
	// extension dir (#610) — none of which CI exercises against real S3.
	//
	// 1.4 LTS support ENDS 2026-09-16. Past that date this pin is a known
	// liability rather than a preference: either move to the next LTS or record
	// an explicit decision to run an unsupported engine. There is no 1.6 on the
	// release calendar and DuckDB's stated policy is "every other release is
	// LTS", so the next LTS is EXPECTED to be 2.0.0 (Fall 2026, tentative) —
	// but 2.0.0 carries no LTS marker yet, so confirm before relying on it.
	github.com/duckdb/duckdb-go/v2 v2.5.6
	// >= v1.15.0 is a data-safety floor, not a preference: through v1.14.0,
	// MariadbGTIDSet.AddSet stored the EVENT'S OWN *MariadbGTID pointer into
	// the syncer's accumulated set on a domain's first appearance, and the
	// next event of that domain forward()ed — mutated — it while the consumer
	// goroutine could still be reading the delivered event (data race; torn
	// GTIDs could reach the checkpoint). v1.15.0 refactored AddSet to store a
	// Clone, removing the shared write entirely. Never downgrade below it.
	github.com/go-mysql-org/go-mysql v1.16.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/google/uuid v1.6.0
	github.com/jackc/pglogrepl v0.0.0-20260401131349-e37c41485510
	github.com/jackc/pgx/v5 v5.10.0
	github.com/modelcontextprotocol/go-sdk v1.3.1
	github.com/parquet-go/parquet-go v0.24.0
	github.com/prometheus/client_golang v1.23.2
	github.com/spf13/cobra v1.10.2
	go.yaml.in/yaml/v2 v2.4.2
	golang.org/x/crypto v0.40.0
	golang.org/x/sync v0.20.0
	golang.org/x/term v0.39.0
	golang.org/x/text v0.36.0
)

require (
	github.com/prometheus/client_model v0.6.2
	github.com/spf13/pflag v1.0.9
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/arrow-go/v18 v18.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.7 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/duckdb/duckdb-go-bindings v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.3.5 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/pingcap/errors v0.11.5-0.20260310054046-9c8b3586e4b2 // indirect
	github.com/pingcap/log v1.1.1-0.20260227082333-572e590d08f1 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260504140133-511dba1dbe17 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/telemetry v0.0.0-20260311193753-579e4da9a98c // indirect
	golang.org/x/tools v0.43.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
