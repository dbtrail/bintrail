package cli

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/verify"
)

// pgLiveVerifyConnect opens the live PostgreSQL source for `verify
// --source-dsn` against a PG-flavored index and returns the fingerprint
// provider plus its close func. It is nil in the CORE bintrail binary on
// purpose: the provider (internal/pgverifysource) links the PostgreSQL driver
// stack, which cliapp's TestCoreBinaryIsPostgresFree bans from cmd/bintrail —
// PG capture, and with it PG live-source verify, belongs to cmd/bintrail-pg.
// With the seam empty, runVerifyLivePG refuses with a message that names the
// binary that can do it, instead of a link-time dependency violation.
var pgLiveVerifyConnect func(ctx context.Context, dsn string) (verify.PGSourceChecksum, func() error, error)

// SetPGLiveVerifyConnect installs the PostgreSQL live-source provider —
// called once at startup by pgx-carrying binaries (cmd/bintrail-pg passes
// pgverifysource.LiveSource). Not a general plugin point: the one intended
// implementation is internal/pgverifysource, and the indirection exists only
// so this package can stay pgx-free (see pgLiveVerifyConnect).
func SetPGLiveVerifyConnect(connect func(ctx context.Context, dsn string) (verify.PGSourceChecksum, func() error, error)) {
	pgLiveVerifyConnect = connect
}

// PGLiveVerifySeamFilled reports whether a provider was installed. It exists
// for ONE consumer: cmd/bintrail-pg's wiring test, which asserts its init()
// actually filled the seam — deleting the SetPGLiveVerifyConnect call there
// would otherwise leave every suite green while `bintrail-pg verify
// --source-dsn` refused at runtime with a message pointing at... itself.
func PGLiveVerifySeamFilled() bool {
	return pgLiveVerifyConnect != nil
}
