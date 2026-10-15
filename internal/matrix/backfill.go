package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"maunium.net/go/mautrix"
)

type messagesResponse struct {
	Chunk []json.RawMessage `json:"chunk"`
	State []json.RawMessage `json:"state"`
	Start string            `json:"start"`
	End   string            `json:"end"`
}

// Hydrate performs the initial backfill of a room the first time it is viewed.
// Rooms the account isn't joined to are loaded through the Synapse admin API.
func (c *Client) Hydrate(ctx context.Context, roomID string) error {
	c.store.EnsureRoom(roomID)
	info, _ := c.store.Room(roomID)
	if info.Hydrated {
		return nil
	}
	if !info.Joined && c.IsAdmin() && info.Name == "" {
		if ri, err := c.adminRoomInfo(ctx, roomID); err == nil {
			c.store.UpdateRoom(roomID, func(r *dagRoom) {
				r.Name = firstNonEmpty(ri.Name, string(ri.CanonicalAlias))
				r.Version = ri.Version
			})
		}
	}
	if _, err := c.Backfill(ctx, roomID, c.cfg.InitialBackfill); err != nil {
		return err
	}
	c.store.UpdateRoom(roomID, func(r *dagRoom) { r.Hydrated = true })
	c.kickCrawl(roomID)
	return nil
}

// Backfill paginates /messages backwards, adding up to count events. It
// returns how many new events were added to the graph.
func (c *Client) Backfill(ctx context.Context, roomID string, count int) (int, error) {
	lock := c.roomLock(roomID)
	lock.Lock()
	defer lock.Unlock()

	info, _ := c.store.Room(roomID)
	useAdmin := !info.Joined
	if useAdmin && !c.IsAdmin() {
		return 0, fmt.Errorf("not joined to %s and not a Synapse admin", roomID)
	}

	added, seen := 0, 0
	for seen < count {
		info, _ = c.store.Room(roomID)
		if info.BackfillDone {
			break
		}
		query := map[string]string{
			"dir":    "b",
			"limit":  strconv.Itoa(min(c.cfg.BackfillPageSize, count-seen)),
			"filter": federationFilter,
		}
		if info.PrevBatch != "" {
			query["from"] = info.PrevBatch
		}
		var path mautrix.PrefixableURLPath = mautrix.ClientURLPath{"v3", "rooms", roomID, "messages"}
		if useAdmin {
			path = mautrix.SynapseAdminURLPath{"v1", "rooms", roomID, "messages"}
		}
		var resp messagesResponse
		if err := c.get(ctx, c.cli.BuildURLWithQuery(path, query), &resp); err != nil {
			if useAdmin && isForbidden(err) {
				c.admin.Store(false)
			}
			return added, fmt.Errorf("fetching messages: %w", err)
		}
		// State comes from anywhere in history, so only the chunk moves the floor.
		chunkNodes, _ := c.store.AddTimeline(roomID, resp.Chunk)
		stateNodes, _ := c.store.Add(roomID, resp.State)
		added += len(chunkNodes) + len(stateNodes)
		seen += len(resp.Chunk)

		done := resp.End == "" || len(resp.Chunk) == 0
		c.store.UpdateRoom(roomID, func(r *dagRoom) {
			if done {
				r.BackfillDone = true
			} else {
				r.PrevBatch = resp.End
			}
		})
		if done {
			break
		}
	}
	c.log.Debug().Str("room_id", roomID).Int("added", added).Msg("Backfilled")
	if added > 0 {
		c.kickCrawl(roomID)
	}
	return added, nil
}

// ResolveEvent fetches a single referenced-but-unknown event through the
// Synapse admin fetch_event API. It returns how many events are still missing.
func (c *Client) ResolveEvent(ctx context.Context, roomID, eventID string) (remaining int, err error) {
	if !c.IsAdmin() {
		return 0, fmt.Errorf("resolving missing events requires Synapse admin")
	}
	lock := c.roomLock(roomID)
	lock.Lock()
	defer lock.Unlock()

	if !slices.Contains(c.store.MissingRefs(roomID), eventID) {
		return 0, fmt.Errorf("%s is not a missing event of %s", eventID, roomID)
	}
	raw, err := c.FetchEvent(ctx, eventID)
	if err != nil {
		if isForbidden(err) {
			c.admin.Store(false)
		}
		return len(c.store.MissingRefs(roomID)), fmt.Errorf("fetching %s: %w", eventID, err)
	}
	if _, errs := c.store.Add(roomID, []json.RawMessage{raw}, eventID); len(errs) > 0 {
		return len(c.store.MissingRefs(roomID)), errs[0]
	}
	return len(c.store.MissingRefs(roomID)), nil
}

type fetchResult struct {
	raw json.RawMessage
	err error
}

const fetchConcurrency = 8

func (c *Client) fetchEvents(ctx context.Context, ids []string) []fetchResult {
	results := make([]fetchResult, len(ids))
	sem := make(chan struct{}, fetchConcurrency)
	done := make(chan struct{})
	for i, eventID := range ids {
		go func() {
			sem <- struct{}{}
			defer func() { <-sem; done <- struct{}{} }()
			raw, err := c.FetchEvent(ctx, eventID)
			results[i] = fetchResult{raw: raw, err: err}
		}()
	}
	for range ids {
		<-done
	}
	return results
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
