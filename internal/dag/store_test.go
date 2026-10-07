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
