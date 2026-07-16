package ext

import "context"

// DoctorCheck is one preflight check result produced by a registered doctor
// check function. Status must be one of the internal/doctor.CheckStatus
// strings — "pass", "fail", "warn", or "skip"; the core coerces an unknown
// value to a warn with a note so a drifting extension cannot silently
// miscount the report.
type DoctorCheck struct {
	Name        string
	Status      string
	Detail      string
	Remediation string
}

// doctorChecks is empty in the OSS build — RunDoctorChecks returns nothing.
var doctorChecks []func(ctx context.Context, sourceDSN, indexDSN string) []DoctorCheck

// RegisterDoctorCheck registers a function that contributes extra checks to
// `bintrail doctor` and to preflight surfaces built on it (`bintrail up`).
// Same startup-only contract as the other seams: call from main() before
// command dispatch. Functions run in registration order.
func RegisterDoctorCheck(fn func(ctx context.Context, sourceDSN, indexDSN string) []DoctorCheck) {
	doctorChecks = append(doctorChecks, fn)
}

// RunDoctorChecks invokes every registered doctor check function and returns
// the concatenated results. Called by the core after its own checks have run;
// registered checks are advisory extensions of the report, and like the
// built-in checks they must probe (validate, never set). Safe to call with
// nothing registered — returns nil.
func RunDoctorChecks(ctx context.Context, sourceDSN, indexDSN string) []DoctorCheck {
	var out []DoctorCheck
	for _, fn := range doctorChecks {
		out = append(out, fn(ctx, sourceDSN, indexDSN)...)
	}
	return out
}
