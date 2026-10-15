package dag

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

func pdu(id string, depth int64, prev []string, auth []string) json.RawMessage {
	data, _ := json.Marshal(map[string]any{
		"event_id": id, "type": "m.room.message", "sender": "@a:x", "depth": depth,
		"origin_server_ts": depth * 1000, "prev_events": prev, "auth_events": auth,
		"content": map[string]any{"body": "hi " + id},
	})
	return data
}

func TestParseRefsBothFormats(t *testing.T) {
	v1 := json.RawMessage(`[["$a:x", {"sha256": "abc"}], ["$b:x", {}]]`)
	v3 := json.RawMessage(`["$a", "$b"]`)
	for _, raw := range []json.RawMessage{v1, v3} {
		refs, err := parseRefs(raw)
		if err != nil || len(refs) != 2 {
			t.Fatalf("parseRefs(%s) = %v, %v", raw, refs, err)
		}
	}
}

func TestSnapshotMissingAndExtremities(t *testing.T) {
	s := NewStore(100)
	// $gone is referenced but never added; $c and $d fork from $b.
	s.Add("!r", []json.RawMessage{
		pdu("$a", 1, []string{"$gone"}, nil),
		pdu("$b", 2, []string{"$a"}, []string{"$a"}),
		pdu("$c", 3, []string{"$b"}, nil),
		pdu("$d", 3, []string{"$b"}, nil),
	})
	g, ok := s.Snapshot("!r", 0)
	if !ok {
		t.Fatal("room not found")
	}
	if len(g.Nodes) != 4 || g.Nodes[0].ID != "$a" {
		t.Fatalf("unexpected nodes: %+v", g.Nodes)
	}
	if !slices.Equal(g.Missing, []string{"$gone"}) {
		t.Errorf("missing = %v", g.Missing)
	}
	if !slices.Equal(g.Extremities, []string{"$c", "$d"}) {
		t.Errorf("extremities = %v", g.Extremities)
	}

	// With a limit, events cut off are neither shown nor reported missing.
	g, _ = s.Snapshot("!r", 2)
	if !g.Truncated || len(g.Nodes) != 2 || len(g.Missing) != 0 {
		t.Errorf("truncated snapshot: truncated=%v nodes=%d missing=%v", g.Truncated, len(g.Nodes), g.Missing)
	}
}

func TestAddDeduplicatesAndPublishes(t *testing.T) {
	s := NewStore(100)
	ch, cancel := s.Subscribe("!r")
	defer cancel()
	s.Add("!r", []json.RawMessage{pdu("$a", 1, nil, nil)})
	added, _ := s.Add("!r", []json.RawMessage{pdu("$a", 1, nil, nil), pdu("$b", 2, []string{"$a"}, nil)})
	if len(added) != 1 || added[0].ID != "$b" {
		t.Fatalf("added = %+v", added)
	}
	if u := <-ch; len(u.Nodes) != 1 || u.Nodes[0].ID != "$a" {
		t.Errorf("first update = %+v", u)
	}
	if u := <-ch; len(u.Nodes) != 1 || u.Nodes[0].ID != "$b" {
		t.Errorf("second update = %+v", u)
	}
}

func TestEvictKeepsNewestAndCreate(t *testing.T) {
	s := NewStore(3)
	create, _ := json.Marshal(map[string]any{
		"event_id": "$create", "type": "m.room.create", "state_key": "", "depth": 1,
		"content": map[string]any{"room_version": "12"}, "prev_events": []string{}, "auth_events": []string{},
	})
	events := []json.RawMessage{create}
	for i := 2; i <= 6; i++ {
		events = append(events, pdu(fmt.Sprintf("$e%d", i), int64(i), nil, nil))
	}
	s.Add("!r", events)
	g, _ := s.Snapshot("!r", 0)
	var ids []string
	for _, n := range g.Nodes {
		ids = append(ids, n.ID)
	}
	if !slices.Equal(ids, []string{"$create", "$e5", "$e6"}) {
		t.Errorf("kept %v", ids)
	}
	if g.Room.Version != "12" || !g.Room.BackfillDone {
		t.Errorf("room info = %+v", g.Room)
	}
}

func TestFallbackID(t *testing.T) {
	raw := json.RawMessage(`{"type":"m.room.member","state_key":"@a:x","sender":"@a:x","depth":5,"prev_events":[],"auth_events":[],"content":{"membership":"join","displayname":"A"}}`)
	n, err := ParsePDU(raw, "$fetched")
	if err != nil || n.ID != "$fetched" || n.Summary != "join @a:x (A)" {
		t.Fatalf("ParsePDU = %+v, %v", n, err)
	}
}

func TestGapsBoundedByFloorNewestFirst(t *testing.T) {
	s := NewStore(100)
	// Old state from /messages "state": far below the timeline window.
	s.Add("!r", []json.RawMessage{pdu("$oldstate", 5, []string{"$ancient"}, nil)})
	s.AddTimeline("!r", []json.RawMessage{
		pdu("$t1", 100, []string{"$below"}, nil),
		pdu("$t2", 101, []string{"$t1"}, nil),
		pdu("$t3", 105, []string{"$t2", "$dummy"}, []string{"$member"}),
		pdu("$t4", 103, []string{"$hole"}, nil),
	})
	if info, _ := s.Room("!r"); info.Floor != 100 {
		t.Fatalf("floor = %d, want 100", info.Floor)
	}
	got := s.Gaps("!r")
	// $below is the floor event's parent: older history, not a hole.
	want := []string{"$dummy", "$hole"}
	if !slices.Equal(got, want) {
		t.Fatalf("gaps = %v, want %v (no auth refs, nothing at or below the floor)", got, want)
	}
	// Filling a gap with an event below the floor doesn't open new gaps.
	s.Add("!r", []json.RawMessage{pdu("$hole", 99, []string{"$deeper"}, nil)})
	if got := s.Gaps("!r"); !slices.Equal(got, []string{"$dummy"}) {
		t.Fatalf("gaps = %v, want [$dummy]", got)
	}
}

func TestEvictionRaisesFloor(t *testing.T) {
	s := NewStore(3)
	var evs []json.RawMessage
	for i := range 5 {
		evs = append(evs, pdu(fmt.Sprintf("$e%d", i), int64(10+i), []string{fmt.Sprintf("$e%d", i-1)}, nil))
	}
	s.AddTimeline("!r", evs)
	if info, _ := s.Room("!r"); info.Floor != 12 {
		t.Fatalf("floor = %d, want 12 after evicting the two oldest", info.Floor)
	}
	if got := s.Gaps("!r"); len(got) != 0 {
		t.Fatalf("gaps = %v, would re-fetch evicted events", got)
	}
}
