package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// The client-server API hides some events from /messages and /sync, notably
// org.matrix.dummy_event, which servers send to merge forward extremities. A
// room loaded through it is therefore full of holes exactly where its forks
// join. The gap crawler fills them by walking prev_events backwards through
// the Synapse admin fetch_event API, newest first, down to the room's Floor
// (the oldest loaded timeline event), so it always terminates.

// crawlBatch caps how many events are fetched between store updates.
const crawlBatch = 50

// crawlPause spaces out background rounds so a large window doesn't hammer
// the homeserver.
const crawlPause = time.Second

type crawlState struct {
	wake chan struct{}

	mu sync.Mutex
	// failed are events fetch_event could not return; they stay missing.
	failed map[string]struct{}
}

func (c *Client) crawl(roomID string) *crawlState {
	if st, ok := c.crawls.Load(roomID); ok {
		return st.(*crawlState)
	}
	st, loaded := c.crawls.LoadOrStore(roomID, &crawlState{
		wake:   make(chan struct{}, 1),
		failed: make(map[string]struct{}),
	})
	if !loaded && c.cfg.FillGaps && c.ctx != nil {
		go c.crawlLoop(roomID, st.(*crawlState))
	}
	return st.(*crawlState)
}

// kickCrawl schedules a background gap fill of a room.
func (c *Client) kickCrawl(roomID string) {
	if !c.cfg.FillGaps || !c.IsAdmin() {
		return
	}
	select {
	case c.crawl(roomID).wake <- struct{}{}:
	default: // a round is already pending
	}
}

func (c *Client) crawlLoop(roomID string, st *crawlState) {
	ctx := c.ctx
	for {
		select {
		case <-ctx.Done():
			return
		case <-st.wake:
		}
		if !c.IsAdmin() {
			continue
		}
		resolved, remaining, err := c.FillGaps(ctx, roomID, c.cfg.ResolveBudget)
		log := c.log.Debug()
		if err != nil {
			log = c.log.Warn().Err(err)
		}
		if resolved > 0 || err != nil {
			log.Str("room_id", roomID).Int("resolved", resolved).Int("remaining", remaining).Msg("Filled DAG gaps")
		}
		if err == nil && resolved > 0 && remaining > 0 {
			select {
			case <-time.After(crawlPause):
				c.kickCrawl(roomID)
			case <-ctx.Done():
				return
			}
		}
	}
}

// FillGaps fetches up to budget missing prev_events of the loaded window,
// newest first. It returns how many events were added and how many gaps are
// left that may still be fetchable.
func (c *Client) FillGaps(ctx context.Context, roomID string, budget int) (resolved, remaining int, err error) {
	if !c.IsAdmin() {
		return 0, 0, fmt.Errorf("resolving missing events requires Synapse admin")
	}
	st := c.crawl(roomID)
	for budget > 0 && ctx.Err() == nil {
		batch := c.pendingGaps(roomID, st, min(budget, crawlBatch))
		if len(batch) == 0 {
			break
		}
		budget -= len(batch)
		n, err := c.fetchInto(ctx, roomID, st, batch)
		resolved += n
		if err != nil {
			return resolved, len(c.pendingGaps(roomID, st, 0)), err
		}
	}
	return resolved, len(c.pendingGaps(roomID, st, 0)), ctx.Err()
}

// pendingGaps returns up to limit (0 = all) gaps not known to be unfetchable.
func (c *Client) pendingGaps(roomID string, st *crawlState, limit int) []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []string
	for _, id := range c.store.Gaps(roomID) {
		if _, bad := st.failed[id]; bad {
			continue
		}
		out = append(out, id)
		if len(out) == limit {
			break
		}
	}
	return out
}

// fetchInto fetches ids into the room. Every id ends up either in the store
// or in st.failed, so repeated calls always make progress.
func (c *Client) fetchInto(ctx context.Context, roomID string, st *crawlState, ids []string) (int, error) {
	results := c.fetchEvents(ctx, ids)
	var events []json.RawMessage
	var eventIDs []string
	var fatal error
	for i, res := range results {
		if res.err == nil && !inRoom(res.raw, roomID) {
			res.err = fmt.Errorf("event belongs to another room")
		}
		if res.err != nil {
			if isForbidden(res.err) {
				c.admin.Store(false)
				fatal = res.err
				continue
			}
			c.log.Debug().Err(res.err).Str("event_id", ids[i]).Msg("fetch_event failed")
			continue
		}
		events = append(events, res.raw)
		eventIDs = append(eventIDs, ids[i])
	}
	lock := c.roomLock(roomID)
	lock.Lock()
	added, _ := c.store.Add(roomID, events, eventIDs...)
	lock.Unlock()
	if fatal != nil {
		return len(added), fatal
	}
	if ctx.Err() != nil {
		// Cancelled fetches say nothing about whether the event exists.
		return len(added), ctx.Err()
	}
	st.mu.Lock()
	for _, id := range ids {
		if _, ok := c.store.Raw(roomID, id); !ok {
			st.failed[id] = struct{}{}
		}
	}
	st.mu.Unlock()
	return len(added), nil
}

func inRoom(raw json.RawMessage, roomID string) bool {
	var ev struct {
		RoomID string `json:"room_id"`
	}
	return json.Unmarshal(raw, &ev) == nil && ev.RoomID == roomID
}
