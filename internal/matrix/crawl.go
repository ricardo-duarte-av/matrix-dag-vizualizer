package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ricardo-duarte-av/matrix-dag-vizualizer/internal/dag"
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

// canCrawl reports whether the crawler has any way to fetch events.
func (c *Client) canCrawl() bool {
	return c.IsAdmin() || (c.fed != nil && c.cfg.Federation.Crawl)
}

// kickCrawl schedules a background gap fill of a room.
func (c *Client) kickCrawl(roomID string) {
	if !c.cfg.FillGaps || !c.canCrawl() {
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
		if !c.canCrawl() {
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
	if !c.canCrawl() {
		return 0, 0, errCantFetch
	}
	st := c.crawl(roomID)
	for budget > 0 && ctx.Err() == nil {
		batch := c.pendingGaps(roomID, st, min(budget, crawlBatch))
		if len(batch) == 0 {
			break
		}
		budget -= len(batch)
		added, err := c.fetchInto(ctx, roomID, st, batch, c.cfg.Federation.Crawl)
		resolved += len(added)
		if err != nil {
			c.store.Notify(roomID, fmt.Sprintf("Gap crawl stopped: %v", err))
			return resolved, len(c.pendingGaps(roomID, st, 0)), err
		}
		dummies, remote := 0, 0
		for _, n := range added {
			if n.Type == "org.matrix.dummy_event" {
				dummies++
			}
			if n.Source != "" {
				remote++
			}
		}
		msg := fmt.Sprintf("Gap crawl: requested %d, added %d (%d dummy events", len(batch), len(added), dummies)
		if remote > 0 {
			msg += fmt.Sprintf(", %d over federation", remote)
		}
		c.store.Notify(roomID, msg+fmt.Sprintf("), %d unavailable; %d gaps left",
			len(batch)-len(added), len(c.pendingGaps(roomID, st, 0))))
	}
	remaining = len(c.pendingGaps(roomID, st, 0))
	if remaining == 0 && resolved > 0 {
		info, _ := c.store.Room(roomID)
		msg := fmt.Sprintf("Gap crawl done: every hole above depth %d is filled", info.Floor)
		if unavailable := len(c.store.Gaps(roomID)); unavailable > 0 {
			msg = fmt.Sprintf("Gap crawl done: %d events above depth %d couldn't be fetched and stay missing", unavailable, info.Floor)
		}
		c.store.Notify(roomID, msg)
	}
	return resolved, remaining, ctx.Err()
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

// errCantFetch means neither Synapse admin nor federation fetching is available.
var errCantFetch = errors.New("fetching missing events requires Synapse admin or federation.signing_key_path")

// ResolveEvents fetches specific missing events of a room: from the
// homeserver (Synapse admin) with a federation fallback, or, when server is
// set, from that server only. It returns the events that are still missing.
func (c *Client) ResolveEvents(ctx context.Context, roomID string, ids []string, server string) (added []*dag.Node, missing []string, err error) {
	if server != "" && c.fed == nil {
		return nil, ids, errors.New("federation fetching is not configured")
	}
	if !c.IsAdmin() && c.fed == nil {
		return nil, ids, errCantFetch
	}
	refs := c.store.MissingRefs(roomID)
	for _, id := range ids {
		if !slices.Contains(refs, id) {
			return nil, ids, fmt.Errorf("%s is not a missing event of %s", id, roomID)
		}
	}
	st := c.crawl(roomID)
	// A manual request retries events earlier attempts gave up on.
	st.mu.Lock()
	for _, id := range ids {
		delete(st.failed, id)
	}
	st.mu.Unlock()

	if server != "" {
		c.store.Notify(roomID, fmt.Sprintf("Fetching %d event(s) from %s over federation", len(ids), server))
		added = c.fetchRemote(ctx, roomID, ids, []string{server})
		c.markFailed(roomID, st, ids)
	} else {
		c.store.Notify(roomID, fmt.Sprintf("Resolving %d event(s)", len(ids)))
		added, err = c.fetchInto(ctx, roomID, st, ids, true)
	}
	for _, id := range ids {
		if _, ok := c.store.Raw(roomID, id); !ok {
			missing = append(missing, id)
		}
	}
	msg := fmt.Sprintf("Resolved %d of %d event(s)", len(ids)-len(missing), len(ids))
	if len(missing) > 0 {
		msg += fmt.Sprintf("; %d not available", len(missing))
	}
	c.store.Notify(roomID, msg)
	return added, missing, err
}

// fetchInto fetches ids into the room: through Synapse admin when available,
// then (if remote) over federation for whatever the homeserver doesn't have.
// Every id ends up either in the store or in st.failed, so repeated calls
// always make progress.
func (c *Client) fetchInto(ctx context.Context, roomID string, st *crawlState, ids []string, remote bool) ([]*dag.Node, error) {
	var added []*dag.Node
	notLocal := ids
	if c.IsAdmin() {
		notLocal = nil
		results := c.fetchEvents(ctx, ids)
		var events []json.RawMessage
		var eventIDs []string
		var fatal error
		for i, res := range results {
			if res.err == nil && !inRoom(res.raw, roomID) {
				c.log.Debug().Str("event_id", ids[i]).Msg("fetch_event returned an event of another room")
				continue
			}
			if res.err != nil {
				if isForbidden(res.err) {
					c.admin.Store(false)
					fatal = res.err
					continue
				}
				c.log.Debug().Err(res.err).Str("event_id", ids[i]).Msg("fetch_event failed")
				notLocal = append(notLocal, ids[i])
				continue
			}
			events = append(events, res.raw)
			eventIDs = append(eventIDs, ids[i])
		}
		lock := c.roomLock(roomID)
		lock.Lock()
		added, _ = c.store.Add(roomID, events, eventIDs...)
		lock.Unlock()
		if fatal != nil {
			return added, fatal
		}
	}
	if remote && c.fed != nil && len(notLocal) > 0 && ctx.Err() == nil {
		added = append(added, c.fetchRemote(ctx, roomID, notLocal, c.fedServers)...)
	}
	if ctx.Err() != nil {
		// Cancelled fetches say nothing about whether the event exists.
		return added, ctx.Err()
	}
	c.markFailed(roomID, st, ids)
	return added, nil
}

// fetchRemote fetches ids over federation, asking servers in order.
func (c *Client) fetchRemote(ctx context.Context, roomID string, ids []string, servers []string) []*dag.Node {
	var (
		mu    sync.Mutex
		added []*dag.Node
		wg    sync.WaitGroup
		sem   = make(chan struct{}, fetchConcurrency)
	)
	for _, eventID := range ids {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			raw, server, err := c.FetchRemote(ctx, roomID, eventID, servers)
			if err != nil {
				c.log.Debug().Err(err).Str("event_id", eventID).Msg("Federation fetch failed")
				return
			}
			lock := c.roomLock(roomID)
			lock.Lock()
			nodes, _ := c.store.AddRemote(roomID, server, []json.RawMessage{raw}, []string{eventID})
			lock.Unlock()
			mu.Lock()
			added = append(added, nodes...)
			mu.Unlock()
		})
	}
	wg.Wait()
	return added
}

// markFailed records which ids are still not in the store.
func (c *Client) markFailed(roomID string, st *crawlState, ids []string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, id := range ids {
		if _, ok := c.store.Raw(roomID, id); !ok {
			st.failed[id] = struct{}{}
		}
	}
}

func inRoom(raw json.RawMessage, roomID string) bool {
	var ev struct {
		RoomID string `json:"room_id"`
	}
	return json.Unmarshal(raw, &ev) == nil && ev.RoomID == roomID
}
