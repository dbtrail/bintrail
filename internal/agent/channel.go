package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ─── Configuration ───────────────────────────────────────────────────────────

// ChannelConfig holds the settings for connecting to dbtrail.
type ChannelConfig struct {
	// Endpoint is the WebSocket URL to connect to
	// (e.g. "wss://api.dbtrail.io/v1/agent").
	Endpoint string

	// APIKey authenticates the agent to dbtrail.
	APIKey string

	// Version is the bintrail agent version reported in heartbeats.
	Version string

	// BintrailID is the server identity reported in heartbeats.
	BintrailID string

	// HeartbeatInterval controls how often heartbeats are sent.
	// Zero defaults to 30 seconds.
	HeartbeatInterval time.Duration

	// MaxReconnectDelay caps the exponential backoff. Zero defaults to 60s.
	MaxReconnectDelay time.Duration
}

func (c *ChannelConfig) heartbeatInterval() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return 30 * time.Second
}

func (c *ChannelConfig) maxReconnectDelay() time.Duration {
	if c.MaxReconnectDelay > 0 {
		return c.MaxReconnectDelay
	}
	return 60 * time.Second
}

// ─── Channel ─────────────────────────────────────────────────────────────────

// Channel manages an outbound WebSocket connection to dbtrail. It
// reconnects automatically with exponential backoff and sends periodic
// heartbeats. Incoming commands are dispatched to the provided Handler.
type Channel struct {
	cfg     ChannelConfig
	handler Handler
	startAt time.Time
	logger  *slog.Logger
}

// NewChannel creates a Channel. The handler is called for each command
// received from dbtrail. If logger is nil, slog.Default() is used.
func NewChannel(cfg ChannelConfig, handler Handler, logger *slog.Logger) *Channel {
	if logger == nil {
		logger = slog.Default()
	}
	return &Channel{
		cfg:     cfg,
		handler: handler,
		startAt: time.Now(),
		logger:  logger,
	}
}

// Run connects to dbtrail and enters the listen/dispatch loop. It
// reconnects automatically on failure with exponential backoff (1s, 2s,
// 4s, … capped at MaxReconnectDelay). Run blocks until ctx is cancelled.
func (ch *Channel) Run(ctx context.Context) error {
	delay := time.Second
	for {
		err := ch.connectAndListen(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ch.logger.Warn("connection lost, reconnecting",
			"error", err,
			"delay", delay.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = min(delay*2, ch.cfg.maxReconnectDelay())
	}
}

// connectAndListen dials the WebSocket, starts the heartbeat goroutine,
// and reads commands until the connection drops or ctx is cancelled.
func (ch *Channel) connectAndListen(ctx context.Context) error {
	conn, err := ch.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	ch.logger.Info("connected to dbtrail", "endpoint", ch.cfg.Endpoint)

	// Start heartbeat goroutine. It exits when hbCtx is cancelled (on
	// connection drop or parent ctx cancellation).
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch.heartbeatLoop(hbCtx, conn)
	}()

	// Read and dispatch commands.
	err = ch.listenLoop(ctx, conn)
	hbCancel()
	wg.Wait()
	return err
}

// dial opens the WebSocket connection with the API key in the
// Authorization header.
func (ch *Channel) dial(ctx context.Context) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+ch.cfg.APIKey)

	conn, _, err := websocket.Dial(ctx, ch.cfg.Endpoint, &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// listenLoop reads JSON commands from the WebSocket and dispatches them
// to the handler. Each command is processed sequentially so handlers can
// safely use non-concurrent resources (DB connections, etc.).
func (ch *Channel) listenLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		var cmd Command
		err := readJSON(ctx, conn, &cmd)
		if err != nil {
			return fmt.Errorf("read command: %w", err)
		}

		ch.logger.Info("received command",
			"command_id", cmd.ID,
			"type", cmd.Type)

		resp := dispatch(ctx, ch.handler, cmd)

		if err := writeJSON(ctx, conn, resp); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		if resp.Error != "" {
			ch.logger.Warn("command failed",
				"command_id", cmd.ID,
				"type", cmd.Type,
				"error", resp.Error)
		} else {
			ch.logger.Info("command completed",
				"command_id", cmd.ID,
				"type", cmd.Type)
		}
	}
}

// heartbeatLoop sends periodic heartbeat messages until ctx is cancelled.
func (ch *Channel) heartbeatLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(ch.cfg.heartbeatInterval())
	defer ticker.Stop()

	// Send one immediately on connect.
	if err := ch.sendHeartbeat(ctx, conn); err != nil {
		ch.logger.Warn("heartbeat send failed", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ch.sendHeartbeat(ctx, conn); err != nil {
				ch.logger.Warn("heartbeat send failed", "error", err)
				return
			}
		}
	}
}

func (ch *Channel) sendHeartbeat(ctx context.Context, conn *websocket.Conn) error {
	hb := Heartbeat{
		Type:       "heartbeat",
		Version:    ch.cfg.Version,
		Uptime:     time.Since(ch.startAt).Truncate(time.Second).String(),
		BintrailID: ch.cfg.BintrailID,
		Timestamp:  time.Now().UTC(),
	}
	return writeJSON(ctx, conn, hb)
}

// ─── JSON helpers ────────────────────────────────────────────────────────────

func readJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

// IsTemporary reports whether err is a connection-level error that should
// trigger a reconnect (as opposed to a permanent auth failure).
func IsTemporary(err error) bool {
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case websocket.StatusPolicyViolation, // 1008 — auth rejected
			websocket.StatusInternalError: // 1011 — server error (may recover)
			return closeErr.Code == websocket.StatusInternalError
		}
	}
	return true // network errors are transient
}
