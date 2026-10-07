package dag

import (
	"encoding/json"
	"slices"
	"sync"
)

// Room is the known portion of a single room's event graph.
type Room struct {
	ID      string
	Name    string
	Version string
	Joined  bool

	nodes map[string]*Node
	raw   map[string]json.RawMessage

	// Backfill state for /messages pagination (dir=b).
	PrevBatch    string
	BackfillDone bool
	Hydrated     bool // initial backfill has been performed
}

// Update is pushed to subscribers whenever new events are added to a room.
type Update struct {
	RoomID string  `json:"room_id"`
	Nodes  []*Node `json:"nodes"`
}

// Store is a concurrency-safe collection of rooms.
type Store struct {
	mu       sync.RWMutex
	rooms    map[string]*Room
	maxNodes int

	subMu sync.Mutex
	subs  map[string]map[chan Update]struct{}
}

func NewStore(maxNodesPerRoom int) *Store {
	return &Store{
		rooms:    make(map[string]*Room),
		maxNodes: maxNodesPerRoom,
		subs:     make(map[string]map[chan Update]struct{}),
	}
}

// EnsureRoom returns the room, creating an empty entry if needed.
func (s *Store) EnsureRoom(roomID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensure(roomID)
}

func (s *Store) ensure(roomID string) *Room {
	r, ok := s.rooms[roomID]
	if !ok {
		r = &Room{
			ID:    roomID,
			nodes: make(map[string]*Node),
			raw:   make(map[string]json.RawMessage),
		}
		s.rooms[roomID] = r
	}
	return r
}

// UpdateRoom applies fn to the room (created if missing) under the write lock.
func (s *Store) UpdateRoom(roomID string, fn func(r *Room)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.ensure(roomID))
}

// RoomInfo is a lock-free copy of a room's metadata.
type RoomInfo struct {
	ID           string `json:"room_id"`
	Name         string `json:"name"`
	Version      string `json:"version"`
	Joined       bool   `json:"joined"`
	EventCount   int    `json:"event_count"`
	PrevBatch    string `json:"-"`
	BackfillDone bool   `json:"backfill_done"`
	Hydrated     bool   `json:"-"`
}

func (r *Room) info() RoomInfo {
	return RoomInfo{
		ID: r.ID, Name: r.Name, Version: r.Version, Joined: r.Joined,
		EventCount: len(r.nodes), PrevBatch: r.PrevBatch,
		BackfillDone: r.BackfillDone, Hydrated: r.Hydrated,
	}
}

func (s *Store) Room(roomID string) (RoomInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[roomID]
	if !ok {
		return RoomInfo{}, false
	}
	return r.info(), true
}

func (s *Store) Rooms() []RoomInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RoomInfo, 0, len(s.rooms))
	for _, r := range s.rooms {
		out = append(out, r.info())
	}
	return out
}

// Add inserts events into a room and notifies subscribers of the ones that
// were new. It returns the newly added nodes.
func (s *Store) Add(roomID string, events []json.RawMessage, fallbackIDs ...string) ([]*Node, []error) {
	var errs []error
	s.mu.Lock()
	r := s.ensure(roomID)
	added := make([]*Node, 0, len(events))
	for i, raw := range events {
		fallback := ""
		if i < len(fallbackIDs) {
			fallback = fallbackIDs[i]
		}
		n, err := ParsePDU(raw, fallback)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, exists := r.nodes[n.ID]; exists {
			continue
		}
		r.nodes[n.ID] = n
		r.raw[n.ID] = raw
		added = append(added, n)
		if n.Type == "m.room.create" && n.StateKey != nil && *n.StateKey == "" {
			r.Version = createVersion(raw)
		}
	}
	s.evict(r)
	s.mu.Unlock()

	if len(added) > 0 {
		s.publish(Update{RoomID: roomID, Nodes: added})
	}
	return added, errs
}

func createVersion(raw json.RawMessage) string {
	var ev struct {
		Content struct {
			RoomVersion string `json:"room_version"`
		} `json:"content"`
	}
	_ = json.Unmarshal(raw, &ev)
	if ev.Content.RoomVersion == "" {
		return "1"
	}
	return ev.Content.RoomVersion
}

// evict drops the shallowest events once a room exceeds the node cap. The
// create event is always kept, as it anchors the graph.
func (s *Store) evict(r *Room) {
	excess := len(r.nodes) - s.maxNodes
	if excess <= 0 {
		return
	}
	all := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		if n.Type != "m.room.create" {
			all = append(all, n)
		}
	}
	slices.SortFunc(all, func(a, b *Node) int { return cmpNode(a, b) })
	for _, n := range all[:min(excess, len(all))] {
		delete(r.nodes, n.ID)
		delete(r.raw, n.ID)
	}
	// Older history was dropped, so further backfill would only re-add it.
	r.BackfillDone = true
}

func cmpNode(a, b *Node) int {
	if a.Depth != b.Depth {
		if a.Depth < b.Depth {
			return -1
		}
		return 1
	}
	if a.TS != b.TS {
		if a.TS < b.TS {
			return -1
		}
		return 1
	}
	if a.ID < b.ID {
		return -1
	} else if a.ID > b.ID {
		return 1
	}
	return 0
}

// Raw returns the full PDU JSON of an event.
func (s *Store) Raw(roomID, eventID string) (json.RawMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[roomID]
	if !ok {
		return nil, false
	}
	raw, ok := r.raw[eventID]
	return raw, ok
}

// Graph is a snapshot of (part of) a room's DAG.
type Graph struct {
	Room RoomInfo `json:"room"`
	// Nodes are sorted by depth, oldest first.
	Nodes []*Node `json:"nodes"`
	// Missing are events referenced by Nodes but not present in the store.
	Missing []string `json:"missing"`
	// Extremities are known nodes that no other known node lists in prev_events.
	Extremities []string `json:"extremities"`
	Truncated   bool     `json:"truncated"`
}

// Snapshot returns the most recent `limit` events of a room (limit <= 0 means all).
func (s *Store) Snapshot(roomID string, limit int) (Graph, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[roomID]
	if !ok {
		return Graph{}, false
	}
	nodes := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, cmpNode)
	g := Graph{Room: r.info()}
	if limit > 0 && len(nodes) > limit {
		nodes = nodes[len(nodes)-limit:]
		g.Truncated = true
	}
	g.Nodes = nodes

	included := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		included[n.ID] = struct{}{}
	}
	referenced := make(map[string]struct{})
	for _, n := range r.nodes {
		for _, p := range n.Prev {
			referenced[p] = struct{}{}
		}
	}
	missing := make(map[string]struct{})
	for _, n := range nodes {
		for _, ref := range slices.Concat(n.Prev, n.Auth) {
			if _, ok := included[ref]; ok {
				continue
			}
			// Events we have but cut off by the limit are not "missing".
			if _, have := r.nodes[ref]; !have {
				missing[ref] = struct{}{}
			}
		}
	}
	g.Missing = sortedKeys(missing)
	g.Extremities = []string{}
	for _, n := range nodes {
		if _, ok := referenced[n.ID]; !ok {
			g.Extremities = append(g.Extremities, n.ID)
		}
	}
	return g, true
}

// MissingRefs returns every referenced-but-unknown event ID in a room.
func (s *Store) MissingRefs(roomID string) []string {
	g, ok := s.Snapshot(roomID, 0)
	if !ok {
		return nil
	}
	return g.Missing
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Subscribe returns a channel receiving updates for a room, and a cancel func.
func (s *Store) Subscribe(roomID string) (<-chan Update, func()) {
	ch := make(chan Update, 64)
	s.subMu.Lock()
	if s.subs[roomID] == nil {
		s.subs[roomID] = make(map[chan Update]struct{})
	}
	s.subs[roomID][ch] = struct{}{}
	s.subMu.Unlock()
	return ch, func() {
		s.subMu.Lock()
		delete(s.subs[roomID], ch)
		if len(s.subs[roomID]) == 0 {
			delete(s.subs, roomID)
		}
		s.subMu.Unlock()
	}
}

func (s *Store) publish(u Update) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range s.subs[u.RoomID] {
		select {
		case ch <- u:
		default: // slow consumer; it can reload the full graph
		}
	}
}
