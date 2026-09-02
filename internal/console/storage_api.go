package console

import (
	"net/http"
	"os"
	"path/filepath"
)

// awsCredsDTO reports which ambient AWS credential signals are visible to THIS
// process — presence booleans plus the two non-secret names (profile, region).
// Values are never serialized. An EC2 instance-profile role is invisible
// without an IMDS network call, so "nothing set" here does NOT mean uploads
// will fail — the Storage panel says so.
type awsCredsDTO struct {
	AccessKeyEnv bool   `json:"access_key_env"`       // AWS_ACCESS_KEY_ID is set
	Profile      string `json:"profile,omitempty"`    // AWS_PROFILE (a name, non-secret)
	RegionEnv    string `json:"region_env,omitempty"` // AWS_REGION / AWS_DEFAULT_REGION
	SharedConfig bool   `json:"shared_config"`        // ~/.aws/credentials or config exists
	// ContainerCreds is env-var presence ONLY: probing the ECS endpoint is a
	// network call and a separate decision (#1534) — the card's copy says so
	// instead of claiming the role signs anything.
	ContainerCreds bool `json:"container_creds"` // ECS task-role endpoint variable set
	// The web-identity arm is PROBED rather than presence-asserted (#1534):
	// the SDK's provider needs the token file readable AND AWS_ROLE_ARN, and
	// a stale projected token, an unmounted volume, or a lone env var used to
	// render "Using an IAM role" while nothing signs — on the page an
	// operator opens precisely because S3 is not working.
	WebIdentity              bool `json:"web_identity"`                          // AWS_WEB_IDENTITY_TOKEN_FILE is set
	WebIdentityTokenReadable bool `json:"web_identity_token_readable,omitempty"` // the token file opened for read
	WebIdentityRoleArn       bool `json:"web_identity_role_arn,omitempty"`       // AWS_ROLE_ARN is also set
}

// webIdentityTokenReadable reports whether the IRSA token file can actually
// be opened for reading. Both checks are load-bearing and neither replaces the
// other: os.Open SUCCEEDS on a directory (an operator who points the variable
// at the projected-volume mount instead of the token inside it would be told
// the token is readable while the SDK's read fails with EISDIR) and it BLOCKS
// forever on a FIFO with no writer, which would leak this handler's goroutine
// per request; a stat alone passes a mode-000 file the SDK cannot read. Stat,
// not Lstat: a Kubernetes projected token is a symlink farm
// (token -> ..data/token), and Lstat would report a healthy IRSA mount as
// unreadable.
func webIdentityTokenReadable(path string) bool {
	if path == "" {
		return false
	}
	if !regularFileExists(path) {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

// stagingDTO is the sql-export staging's share of the disk (#1448): the
// builds waiting on the daemon's disk for their download, so the Storage
// page can show space that used to be invisible until someone ran du.
// Present only on a daemon that can build .sql backups.
type stagingDTO struct {
	Dir      string           `json:"dir"`       // where the builds live
	TTLHours float64          `json:"ttl_hours"` // how long a finished build stays downloadable
	Bytes    int64            `json:"bytes"`     // total over every build below
	Builds   []stagedBuildDTO `json:"builds"`
}

type stagedBuildDTO struct {
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"` // resolved from the registry when the entry still exists
	State      string `json:"state"`                 // running | succeeded | failed (removal still owed) | replaced (a previous build a newer one could not remove)
	At         string `json:"at,omitempty"`
	ExpiresAt  string `json:"expires_at,omitempty"`
	// Bytes counts only when BytesKnown; an unmeasurable build is excluded
	// from the total and shown as unknown, never as 0 B.
	Bytes        int64  `json:"bytes"`
	BytesKnown   bool   `json:"bytes_known"`
	StagingError string `json:"staging_error,omitempty"`
}

type storageInfoResponse struct {
	AWS     awsCredsDTO `json:"aws"`
	Staging *stagingDTO `json:"staging,omitempty"`
}

// handleStorageInfo serves GET /api/storage: process-global storage context
// for the console's Storage page. Like GET /api/rotation it is authenticated
// but not monitor-gated — it reads environment presence, nothing per-server.
func (s *Server) handleStorageInfo(w http.ResponseWriter, r *http.Request) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	writeJSON(w, http.StatusOK, storageInfoResponse{AWS: awsCredsDTO{
		AccessKeyEnv: os.Getenv("AWS_ACCESS_KEY_ID") != "",
		Profile:      os.Getenv("AWS_PROFILE"),
		RegionEnv:    region,
		SharedConfig: hasSharedAWSConfig(),
		ContainerCreds: os.Getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") != "" ||
			os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI") != "",
		WebIdentity:              os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "",
		WebIdentityTokenReadable: webIdentityTokenReadable(os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")),
		WebIdentityRoleArn:       os.Getenv("AWS_ROLE_ARN") != "",
	}, Staging: s.stagingInfo()})
}

// stagingInfo builds the staging share of /api/storage; nil when this
// daemon cannot build .sql backups (there is no staging to report).
func (s *Server) stagingInfo() *stagingDTO {
	if s.sqlExport == nil {
		return nil
	}
	info := s.sqlExport.SQLExportStaged()
	out := &stagingDTO{Dir: info.Dir, TTLHours: info.TTL.Hours(), Builds: []stagedBuildDTO{}}
	for _, b := range info.Builds {
		d := stagedBuildDTO{ServerID: b.ServerID, State: b.State, At: b.At, ExpiresAt: b.ExpiresAt,
			Bytes: b.Bytes, BytesKnown: b.BytesKnown, StagingError: b.StagingError}
		if s.cm != nil && s.cm.reg != nil {
			if e, ok := s.cm.reg.Get(b.ServerID); ok {
				d.ServerName = e.Name
			}
		}
		if b.BytesKnown {
			out.Bytes += b.Bytes
		}
		out.Builds = append(out.Builds, d)
	}
	return out
}

// hasSharedAWSConfig reports whether a shared AWS credentials/config file
// exists where the SDK default chain will look, honoring the SDK's own
// path-override env vars.
func hasSharedAWSConfig() bool {
	cred := os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	conf := os.Getenv("AWS_CONFIG_FILE")
	if home, err := os.UserHomeDir(); err == nil {
		if cred == "" {
			cred = filepath.Join(home, ".aws", "credentials")
		}
		if conf == "" {
			conf = filepath.Join(home, ".aws", "config")
		}
	}
	return regularFileExists(cred) || regularFileExists(conf)
}

func regularFileExists(p string) bool {
	if p == "" {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}
