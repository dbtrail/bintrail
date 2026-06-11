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
	AccessKeyEnv   bool   `json:"access_key_env"`       // AWS_ACCESS_KEY_ID is set
	Profile        string `json:"profile,omitempty"`    // AWS_PROFILE (a name, non-secret)
	RegionEnv      string `json:"region_env,omitempty"` // AWS_REGION / AWS_DEFAULT_REGION
	SharedConfig   bool   `json:"shared_config"`        // ~/.aws/credentials or config exists
	ContainerCreds bool   `json:"container_creds"`      // ECS task-role endpoint configured
	WebIdentity    bool   `json:"web_identity"`         // EKS IRSA token file configured
}

type storageInfoResponse struct {
	AWS awsCredsDTO `json:"aws"`
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
		WebIdentity: os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "",
	}})
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
