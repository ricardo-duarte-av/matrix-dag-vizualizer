package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// syncResponse is a minimal /sync response that keeps events as raw PDUs.
type syncResponse struct {
	NextBatch string `json:"next_batch"`
	Rooms     struct {
		Join   map[string]joinedRoom `json:"join"`
		Invite map[string]any        `json:"invite"`
		Leave  map[string]joinedRoom `json:"leave"`
	} `json:"rooms"`
}

type joinedRoom struct {
	Summary struct {
		Heroes []string `json:"m.heroes"`
	} `json:"summary"`
	State struct {
		Events []json.RawMessage `json:"events"`
	} `json:"state"`
	Timeline struct {
		Events    []json.RawMessage `json:"events"`
		Limited   bool              `json:"limited"`
		PrevBatch string            `json:"prev_batch"`
	} `json:"timeline"`
}

func (c *Client) syncFilter() string {
	f := map[string]any{
		"event_format": "federation",
		"presence":     map[string]any{"not_types": []string{"*"}},
		"account_data": map[string]any{"not_types": []string{"*"}},
		"room": map[string]any{
			"timeline":     map[string]any{"limit": 50},
			"state":        map[string]any{"lazy_load_members": true},
			"ephemeral":    map[string]any{"not_types": []string{"*"}},
			"account_data": map[string]any{"not_types": []string{"*"}},
		},
	}
	if len(c.cfg.Rooms) > 0 {
		f["room"].(map[string]any)["rooms"] = c.cfg.Rooms
	}
	data, _ := json.Marshal(f)
	return string(data)
}

// Run syncs until ctx is cancelled, retrying with backoff on errors.
func (c *Client) Run(ctx context.Context) error {
	go c.adminLoop(ctx)

	filter := c.syncFilter()
	since := ""
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timeout := c.cfg.SyncTimeout
		if since == "" {
			timeout = 0
		}
		query := map[string]string{
			"filter":  filter,
			"timeout": strconv.FormatInt(timeout.Milliseconds(), 10),
		}
		if since != "" {
			query["since"] = since
		}
		var resp syncResponse
		_, err := c.cli.MakeFullRequest(ctx, mautrix.FullRequest{
			Method:       "GET",
			URL:          c.cli.BuildURLWithQuery(mautrix.ClientURLPath{"v3", "sync"}, query),
			ResponseJSON: &resp,
			MaxAttempts:  1,
			Client:       c.cli.Client,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, mautrix.MUnknownToken) {
				return err
			}
			c.log.Warn().Err(err).Dur("retry_in", backoff).Msg("Sync failed")
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		initial := since == ""
		c.handleSync(ctx, &resp, initial)
		if initial {
			c.log.Info().Int("rooms", len(resp.Rooms.Join)).Msg("Initial sync complete")
		}
		since = resp.NextBatch
	}
}

func (c *Client) handleSync(ctx context.Context, resp *syncResponse, initial bool) {
	for roomID, jr := range resp.Rooms.Join {
		if !c.Allowed(roomID) {
			continue
		}
		c.store.UpdateRoom(roomID, func(r *dagRoom) {
			r.Joined = true
			if r.PrevBatch == "" && !r.BackfillDone && jr.Timeline.PrevBatch != "" {
				r.PrevBatch = jr.Timeline.PrevBatch
			}
			if name, fallback := roomName(jr); name != "" && (!fallback || r.Name == "") {
				r.Name = name
			}
		})
		events := slices.Concat(jr.State.Events, jr.Timeline.Events)
		if _, errs := c.store.Add(roomID, events); len(errs) > 0 {
			c.log.Debug().Errs("errors", errs).Str("room_id", roomID).Msg("Skipped unparseable events")
		}
		if jr.Timeline.Limited && !initial {
			c.log.Debug().Str("room_id", roomID).Msg("Timeline gap; missing events will show as placeholders")
		}
	}
	for roomID := range resp.Rooms.Leave {
		c.store.UpdateRoom(roomID, func(r *dagRoom) { r.Joined = false })
	}
	if c.cfg.AutoJoinInvites {
		for roomID := range resp.Rooms.Invite {
			if !c.Allowed(roomID) {
				continue
			}
			go func(roomID string) {
				if _, err := c.cli.JoinRoomByID(ctx, id.RoomID(roomID)); err != nil {
					c.log.Warn().Err(err).Str("room_id", roomID).Msg("Failed to accept invite")
				} else {
					c.log.Info().Str("room_id", roomID).Msg("Accepted invite")
				}
			}(roomID)
		}
	}
}

// roomName derives a display name from name/alias state in the sync batch.
// fallback is true when the name was only derived from the room heroes.
func roomName(jr joinedRoom) (name string, fallback bool) {
	var alias string
	for _, raw := range slices.Concat(jr.State.Events, jr.Timeline.Events) {
		var ev struct {
			Type     string  `json:"type"`
			StateKey *string `json:"state_key"`
			Content  struct {
				Name  string `json:"name"`
				Alias string `json:"alias"`
			} `json:"content"`
		}
		if json.Unmarshal(raw, &ev) != nil || ev.StateKey == nil || *ev.StateKey != "" {
			continue
		}
		switch ev.Type {
		case "m.room.name":
			name = ev.Content.Name
		case "m.room.canonical_alias":
			alias = ev.Content.Alias
		}
	}
	switch {
	case name != "":
		return name, false
	case alias != "":
		return alias, false
	case len(jr.Summary.Heroes) > 0:
		return strings.Join(jr.Summary.Heroes, ", "), true
	}
	return "", false
}
