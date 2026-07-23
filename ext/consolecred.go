package ext

// Credential is what a ConsoleCredentialProvider returns on a SUCCESSFUL
// verification. Identity is a display/log string (e.g. a username or email);
// it is logged, never stored. Policy is the optional access scope to attach to
// the session the console mints (see AccessPolicy) — nil for a full-access
// session, exactly what the built-in single-user login mints.
type Credential struct {
	Identity string
	Policy   *AccessPolicy
}

// ConsoleCredentialProvider replaces the credential check behind the console's
// built-in username/password login form. It is the seam an embedding EE build
// uses to serve that form from a multi-user store (or a directory service)
// instead of the single-user auth file, while the console keeps ownership of
// everything around the check: the login route, the JSON/content-type gate, the
// request-size cap, the rate limiter, and session minting/lifetime/revocation.
//
// When a backend is installed it SUPERSEDES the built-in auth file for
// /api/auth/login — the file is not consulted for login. (The static token is a
// separate credential and is unaffected; /api/auth/setup stays built-in-only —
// a backend never handles first-run password creation.) An EE backend that
// wants to keep the operator's built-in credential working must incorporate it
// itself.
//
// consoleCred is nil in the OSS build, so the built-in single-user auth file is
// the sole login authority and behavior is unchanged.
type ConsoleCredentialProvider interface {
	// Verify checks (username, password). It returns a non-nil *Credential on
	// success, or nil on ANY failure — bad username, bad password, disabled
	// account, backend unavailable. The console renders one uniform 401 for a nil
	// result, so the backend MUST NOT let the caller distinguish these cases.
	//
	// To preserve the console's anti-enumeration and anti-timing guarantees the
	// backend MUST spend a constant, password-hash-equivalent cost on every call
	// regardless of whether the username exists — mirror how the built-in path
	// runs a full bcrypt compare against a dummy hash for an unknown username.
	// The console's login rate limiter and body/content-type caps run BEFORE
	// Verify, so the backend need not reimplement those.
	Verify(username, password string) *Credential
}

// consoleCred is nil in the OSS build — the console's login form is served only
// by the built-in single-user auth file.
var consoleCred ConsoleCredentialProvider

// SetConsoleCredentialProvider installs the process-wide console credential
// backend. Call once from main() before command dispatch, like SetConsoleAuth:
// the console reads it when the server is constructed, so a later install is
// never picked up.
func SetConsoleCredentialProvider(b ConsoleCredentialProvider) {
	consoleCred = b
}

// ConsoleCredential returns the installed backend, or nil when none is
// installed (the OSS build).
func ConsoleCredential() ConsoleCredentialProvider {
	return consoleCred
}
