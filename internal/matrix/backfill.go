package matrix

import (
	"context"
	"encoding/json"
	"fmt"
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
		newNodes, _ := c.store.Add(roomID, append(resp.Chunk, resp.State...))
		added += len(newNodes)
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
	return added, nil
}

// ResolveMissing fetches referenced-but-unknown events through the Synapse
// admin fetch_event API, walking backwards until budget fetches are spent.
func (c *Client) ResolveMissing(ctx context.Context, roomID string, budget int) (resolved int, remaining int, err error) {
	if !c.IsAdmin() {
		return 0, 0, fmt.Errorf("resolving missing events requires Synapse admin")
	}
	lock := c.roomLock(roomID)
	lock.Lock()
	defer lock.Unlock()

	failed := make(map[string]struct{})
	for budget > 0 && ctx.Err() == nil {
		var batch []string
		for _, id := range c.store.MissingRefs(roomID) {
			if _, bad := failed[id]; !bad {
				batch = append(batch, id)
			}
			if len(batch) == budget {
				break
			}
		}
		if len(batch) == 0 {
			break
		}
		budget -= len(batch)
		results := c.fetchEvents(ctx, batch)
		var events []json.RawMessage
		var ids []string
		for i, res := range results {
			if res.err != nil {
				failed[batch[i]] = struct{}{}
				if isForbidden(res.err) {
					c.admin.Store(false)
					return resolved, len(c.store.MissingRefs(roomID)), res.err
				}
				c.log.Debug().Err(res.err).Str("event_id", batch[i]).Msg("fetch_event failed")
				continue
			}
			events = append(events, res.raw)
			ids = append(ids, batch[i])
		}
		added, _ := c.store.Add(roomID, events, ids...)
		resolved += len(added)
		if len(added) == 0 {
			break
		}
	}
	return resolved, len(c.store.MissingRefs(roomID)), ctx.Err()
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
