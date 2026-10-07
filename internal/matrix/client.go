// Package matrix connects to the homeserver and feeds federation-format events
// into the DAG store.
package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/config"
	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/dag"
)

// federationFilter makes the homeserver return events with prev_events,
// auth_events and depth intact.
const federationFilter = `{"event_format":"federation"}`

type Client struct {
	cfg   config.MatrixConfig
	cli   *mautrix.Client
	store *dag.Store
	log   zerolog.Logger

	admin atomic.Bool

	// allowed restricts the visible rooms when matrix.rooms is set.
	allowed map[string]struct{}

	// roomLocks serializes backfill / resolve work per room.
	roomLocks sync.Map
}

type session struct {
	Homeserver  string      `json:"homeserver"`
	UserID      id.UserID   `json:"user_id"`
	DeviceID    id.DeviceID `json:"device_id"`
	AccessToken string      `json:"access_token"`
}

func New(cfg config.MatrixConfig, store *dag.Store, log zerolog.Logger) *Client {
	c := &Client{cfg: cfg, store: store, log: log}
	if len(cfg.Rooms) > 0 {
		c.allowed = make(map[string]struct{}, len(cfg.Rooms))
		for _, r := range cfg.Rooms {
			c.allowed[r] = struct{}{}
		}
	}
	return c
}

func (c *Client) UserID() id.UserID           { return c.cli.UserID }
func (c *Client) Homeserver() string          { return c.cli.HomeserverURL.String() }
func (c *Client) IsAdmin() bool               { return c.admin.Load() }
func (c *Client) Store() *dag.Store           { return c.store }
func (c *Client) Config() config.MatrixConfig { return c.cfg }

// Allowed reports whether a room may be viewed under the matrix.rooms allowlist.
func (c *Client) Allowed(roomID string) bool {
	if c.allowed == nil {
		return true
	}
	_, ok := c.allowed[roomID]
	return ok
}

func (c *Client) roomLock(roomID string) *sync.Mutex {
	l, _ := c.roomLocks.LoadOrStore(roomID, &sync.Mutex{})
	return l.(*sync.Mutex)
}

func (c *Client) sessionPath() string { return filepath.Join(c.cfg.DataDir, "session.json") }

// Connect resolves the homeserver and logs in, reusing a stored session when possible.
func (c *Client) Connect(ctx context.Context) error {
	hsURL, err := c.resolveHomeserver(ctx)
	if err != nil {
		return err
	}
	userID := id.UserID(c.cfg.UserID)
	c.cli, err = mautrix.NewClient(hsURL, userID, "")
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	c.cli.Log = c.log.With().Str("component", "mautrix").Logger()
	c.cli.DefaultHTTPRetries = 2

	switch {
	case c.cfg.AccessToken != "":
		c.cli.AccessToken = c.cfg.AccessToken
		c.cli.DeviceID = id.DeviceID(c.cfg.DeviceID)
		if err := c.whoami(ctx); err != nil {
			return fmt.Errorf("access_token rejected: %w", err)
		}
	case c.loadSession(ctx, hsURL):
		c.log.Info().Str("device_id", c.cli.DeviceID.String()).Msg("Reusing stored session")
	default:
		if err := c.login(ctx, hsURL); err != nil {
			return err
		}
	}
	c.CheckAdmin(ctx)
	return nil
}

func (c *Client) resolveHomeserver(ctx context.Context) (string, error) {
	hs := strings.TrimRight(c.cfg.Homeserver, "/")
	if strings.HasPrefix(hs, "http://") || strings.HasPrefix(hs, "https://") {
		return hs, nil
	}
	wk, err := mautrix.DiscoverClientAPI(ctx, hs)
	if err != nil {
		return "", fmt.Errorf("discovering homeserver for %s: %w", hs, err)
	}
	if wk == nil || wk.Homeserver.BaseURL == "" {
		return "https://" + hs, nil
	}
	return strings.TrimRight(wk.Homeserver.BaseURL, "/"), nil
}

func (c *Client) whoami(ctx context.Context) error {
	resp, err := c.cli.Whoami(ctx)
	if err != nil {
		return err
	}
	c.cli.UserID = resp.UserID
	if resp.DeviceID != "" {
		c.cli.DeviceID = resp.DeviceID
	}
	return nil
}

func (c *Client) loadSession(ctx context.Context, hsURL string) bool {
	data, err := os.ReadFile(c.sessionPath())
	if err != nil {
		return false
	}
	var s session
	if json.Unmarshal(data, &s) != nil || s.AccessToken == "" ||
		s.UserID != id.UserID(c.cfg.UserID) || s.Homeserver != hsURL {
		return false
	}
	c.cli.AccessToken = s.AccessToken
	c.cli.DeviceID = s.DeviceID
	if err := c.whoami(ctx); err != nil {
		c.log.Warn().Err(err).Msg("Stored session is no longer valid, logging in again")
		c.cli.AccessToken = ""
		return false
	}
	return true
}

func (c *Client) login(ctx context.Context, hsURL string) error {
	localpart, _, err := id.UserID(c.cfg.UserID).Parse()
	if err != nil {
		localpart = c.cfg.UserID
	}
	resp, err := c.cli.Login(ctx, &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: localpart},
		Password:                 c.cfg.Password,
		DeviceID:                 id.DeviceID(c.cfg.DeviceID),
		InitialDeviceDisplayName: c.cfg.DeviceName,
		StoreCredentials:         true,
	})
	if err != nil {
		return fmt.Errorf("logging in: %w", err)
	}
	c.log.Info().Str("device_id", resp.DeviceID.String()).Msg("Logged in")
	data, _ := json.MarshalIndent(session{
		Homeserver: hsURL, UserID: resp.UserID, DeviceID: resp.DeviceID, AccessToken: resp.AccessToken,
	}, "", "  ")
	if err := os.MkdirAll(c.cfg.DataDir, 0o700); err != nil {
		c.log.Warn().Err(err).Msg("Can't create data_dir, session will not be persisted")
		return nil
	}
	if err := os.WriteFile(c.sessionPath(), data, 0o600); err != nil {
		c.log.Warn().Err(err).Msg("Can't persist session")
	}
	return nil
}

// get performs an authenticated GET and decodes the JSON response.
func (c *Client) get(ctx context.Context, url string, out any) error {
	_, err := c.cli.MakeFullRequest(ctx, mautrix.FullRequest{
		Method:       http.MethodGet,
		URL:          url,
		ResponseJSON: out,
		MaxAttempts:  2,
	})
	return err
}

// isForbidden reports whether err is an HTTP 401/403 from the homeserver.
func isForbidden(err error) bool {
	var httpErr mautrix.HTTPError
	if errors.As(err, &httpErr) && httpErr.Response != nil {
		return httpErr.Response.StatusCode == http.StatusForbidden ||
			httpErr.Response.StatusCode == http.StatusUnauthorized
	}
	return false
}

type dagRoom = dag.Room
