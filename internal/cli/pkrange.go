package cli

import (
	"fmt"
	"math/big"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pkRangeFlagHelp is the shared help text of --pk-min/--pk-max on query and
// recover. The cost is stated up front: the cast predicate cannot use the
// key index, so the scan is bounded only by the time window.
const (
	pkMinFlagHelp = "Inclusive lower bound on the primary key, for tables whose primary key is one integer column (signed or unsigned); requires --schema and --table; cannot be combined with --pk or --pks; scans the partitions --since/--until keep, so pair it with a time window"
	pkMaxFlagHelp = "Inclusive upper bound on the primary key; same rules as --pk-min, and either bound works alone"
)

// validatePKRangeFlags is the pre-connect half of --pk-min/--pk-max (#1440):
// the flag pairing rules and the integer syntax of each bound. It returns nil
// when neither flag is set. The table's key shape, which needs the index,
// is checked by resolvePKRange.
func validatePKRangeFlags(minText, maxText, schema, table, pk string, pks []string) (*query.PKRange, error) {
	if minText == "" && maxText == "" {
		return nil, nil
	}
	if schema == "" || table == "" {
		return nil, fmt.Errorf("--pk-min/--pk-max require both --schema and --table")
	}
	if pk != "" || len(pks) > 0 {
		return nil, fmt.Errorf("--pk-min/--pk-max cannot be combined with --pk or --pks; use the range or the exact keys")
	}
	var lo, hi *big.Int
	var err error
	if minText != "" {
		if lo, err = query.ParsePKBound(minText); err != nil {
			return nil, fmt.Errorf("--pk-min: %w", err)
		}
	}
	if maxText != "" {
		if hi, err = query.ParsePKBound(maxText); err != nil {
			return nil, fmt.Errorf("--pk-max: %w", err)
		}
	}
	r, err := query.NewPKRange(lo, hi)
	if err != nil {
		return nil, fmt.Errorf("--pk-min/--pk-max: %w", err)
	}
	return r, nil
}

// resolvePKRange is the post-connect half: it checks the table's primary key
// shape in the schema snapshot and picks the cast from its signedness. The
// resolver is the one the command already loads (or nil with the error that
// stopped it); without a snapshot the range is refused, because the cast
// would be a guess.
func resolvePKRange(resolver *metadata.Resolver, resolverErr error, schema, table string, r *query.PKRange) error {
	if r == nil {
		return nil
	}
	if resolver == nil {
		if resolverErr == nil {
			resolverErr = fmt.Errorf("no schema snapshot loaded")
		}
		return fmt.Errorf("--pk-min/--pk-max need the schema snapshot to check the primary key type: %w", resolverErr)
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return fmt.Errorf("--pk-min/--pk-max: %w", err)
	}
	if err := r.ResolveCast(tm); err != nil {
		return fmt.Errorf("--pk-min/--pk-max: %w", err)
	}
	return nil
}
