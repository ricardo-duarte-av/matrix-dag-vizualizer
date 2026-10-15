package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"
)

// Federation fetching asks other servers in a room for events the homeserver
// never received, by calling GET /_matrix/federation/v1/event/{eventId} with
// requests signed as the homeserver. Servers only answer servers that are in
// the room, so this needs the homeserver's own signing key. Fetched events
// are kept in dagviz's store only; the homeserver doesn't learn about them.

// errNotOnServers is returned when no configured server had an event.
var errNotOnServers = errors.New("not available from any federation server")

func (c *Client) setupFederation() error {
	fc := c.cfg.Federation
	if !fc.Enabled() {
		return nil
	}
	data, err := os.ReadFile(fc.SigningKeyPath)
	if err != nil {
		return fmt.Errorf("reading federation signing key: %w", err)
	}
	// Synapse key files may hold several keys, one per line; the first is current.
	line, _, _ := strings.Cut(strings.TrimSpace(string(data)), "\n")
	key, err := federation.ParseSynapseKey(strings.TrimSpace(line))
	if err != nil {
		return fmt.Errorf("parsing federation signing key: %w", err)
	}
	serverName := fc.ServerName
	if serverName == "" {
		serverName = c.cli.UserID.Homeserver()
	}
	c.fed = federation.NewClient(serverName, key, federation.NewInMemoryCache(), exhttp.SensibleClientSettings)
	c.fedServers = fc.Servers
	if !eventIDsVerifiable {
		c.log.Warn().Msg("Built without GOEXPERIMENT=jsonv2: events fetched over federation can't be checked against their event ID")
	}
	c.log.Info().Str("server_name", serverName).Str("key_id", string(key.ID)).
		Strs("servers", fc.Servers).Msg("Federation fetching enabled")
	return nil
}

// FederationServers lists the servers events can be fetched from, or nil
// when federation fetching is disabled.
func (c *Client) FederationServers() []string {
	if c.fed == nil {
		return nil
	}
	return c.fedServers
}

// FetchRemote asks servers, in order, for an event over federation. The event
// must belong to roomID and, when it can be checked, hash to eventID; a
// server returning anything else is skipped.
func (c *Client) FetchRemote(ctx context.Context, roomID, eventID string, servers []string) (json.RawMessage, string, error) {
	if c.fed == nil {
		return nil, "", errors.New("federation fetching is not configured")
	}
	info, _ := c.store.Room(roomID)
	var errs []error
	for _, server := range servers {
		if server == c.fed.ServerName {
			continue // that's the homeserver, which doesn't have it
		}
		resp, err := c.fed.GetEvent(ctx, server, id.EventID(eventID))
		if err == nil && (resp == nil || len(resp.PDUs) == 0) {
			err = errors.New("empty response")
		}
		if err == nil {
			err = checkRemoteEvent(resp.PDUs[0], roomID, eventID, info.Version)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", server, err))
			continue
		}
		return resp.PDUs[0], server, nil
	}
	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}
	return nil, "", fmt.Errorf("%w: %w", errNotOnServers, errors.Join(errs...))
}

func checkRemoteEvent(raw json.RawMessage, roomID, eventID, roomVersion string) error {
	if !inRoom(raw, roomID) {
		return errors.New("event belongs to another room")
	}
	got, err := computeEventID(raw, roomVersion)
	if errors.Is(err, errUnverifiable) {
		return nil
	} else if err != nil {
		return fmt.Errorf("computing event ID: %w", err)
	} else if got != eventID {
		return fmt.Errorf("server returned %s instead", got)
	}
	return nil
}

// errUnverifiable means an event ID can't be computed in this build or room version.
var errUnverifiable = errors.New("event ID can't be verified")
