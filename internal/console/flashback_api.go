package console

import (
	"net"
	"net/http"
)

// flashbackStatusDTO is the wire view of the embedded MySQL-protocol
// time-travel port (#996) for the Connect page (#1446): whether it is on and
// where it listens. The password that authenticates it is the console token,
// which is never serialized; the username rule (a server's registry id or
// name, "default" for the boot entry) is the same selector the /mcp path
// uses, so the frontend derives it from the server list it already holds.
type flashbackStatusDTO struct {
	// Enabled: this process bound the port (Config.FlashbackListen). False on
	// the standalone serve, which owns no such port, and on a watch daemon
	// that did not opt in.
	Enabled bool `json:"enabled"`
	// Listen is the bind address exactly as configured (host:port).
	Listen string `json:"listen,omitempty"`
	// Host and Port are Listen split for the ready-to-copy mysql line. Host
	// is EMPTY when the bind is on every interface (":3308", "0.0.0.0:3308",
	// "[::]:3308"): the daemon cannot know which name the browser reaches it
	// by, so the page fills in the address it was opened on.
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
}

func (s *Server) flashbackStatus() flashbackStatusDTO {
	if s.flashbackListen == "" {
		return flashbackStatusDTO{}
	}
	dto := flashbackStatusDTO{Enabled: true, Listen: s.flashbackListen}
	// A value net.Listen would have refused (no port) cannot reach here from
	// watch, whose bind failure aborts startup; report the raw address alone
	// rather than guess a split.
	host, port, err := net.SplitHostPort(s.flashbackListen)
	if err != nil {
		return dto
	}
	dto.Port = port
	switch host {
	case "", "0.0.0.0", "::":
		// Wildcard bind: no single host to name.
	default:
		dto.Host = host
	}
	return dto
}

// handleFlashbackGet reports the time-travel port's state and address, never
// the token that authenticates it. Behind tokenMiddleware like every /api
// route; classified settings:read (it is daemon configuration, not row data).
func (s *Server) handleFlashbackGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.flashbackStatus())
}
