// Package dag holds the in-memory event graph of each room.
package dag

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// PDU is the subset of a federation-format event that the visualizer needs.
type PDU struct {
	EventID        string          `json:"event_id"`
	RoomID         string          `json:"room_id"`
	Type           string          `json:"type"`
	StateKey       *string         `json:"state_key"`
	Sender         string          `json:"sender"`
	Depth          int64           `json:"depth"`
	OriginServerTS int64           `json:"origin_server_ts"`
	PrevEvents     json.RawMessage `json:"prev_events"`
	AuthEvents     json.RawMessage `json:"auth_events"`
	Content        json.RawMessage `json:"content"`
	Redacts        string          `json:"redacts"`
}

// Node is what the frontend receives for each event in the graph.
type Node struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	StateKey *string  `json:"state_key,omitempty"`
	Sender   string   `json:"sender"`
	Depth    int64    `json:"depth"`
	TS       int64    `json:"ts"`
	Prev     []string `json:"prev"`
	Auth     []string `json:"auth"`
	Summary  string   `json:"summary,omitempty"`
}

// ParsePDU decodes a raw federation-format event. fallbackID is used when the
// event JSON has no event_id (e.g. the Synapse fetch_event admin API).
func ParsePDU(raw json.RawMessage, fallbackID string) (*Node, error) {
	var p PDU
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	if p.EventID == "" {
		p.EventID = fallbackID
	}
	if p.EventID == "" {
		return nil, fmt.Errorf("event has no event_id")
	}
	prev, err := parseRefs(p.PrevEvents)
	if err != nil {
		return nil, fmt.Errorf("parsing prev_events of %s: %w", p.EventID, err)
	}
	auth, err := parseRefs(p.AuthEvents)
	if err != nil {
		return nil, fmt.Errorf("parsing auth_events of %s: %w", p.EventID, err)
	}
	return &Node{
		ID:       p.EventID,
		Type:     p.Type,
		StateKey: p.StateKey,
		Sender:   p.Sender,
		Depth:    p.Depth,
		TS:       p.OriginServerTS,
		Prev:     prev,
		Auth:     auth,
		Summary:  summarize(&p),
	}, nil
}

// parseRefs handles both reference formats: room versions 1/2 use
// [[event_id, {hashes}], ...], later versions use [event_id, ...].
func parseRefs(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []string{}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		var id string
		if err := json.Unmarshal(item, &id); err == nil {
			out = append(out, id)
			continue
		}
		var pair []json.RawMessage
		if err := json.Unmarshal(item, &pair); err != nil || len(pair) == 0 {
			return nil, fmt.Errorf("unrecognised event reference %s", item)
		}
		if err := json.Unmarshal(pair[0], &id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

const summaryLen = 140

func summarize(p *PDU) string {
	var c map[string]any
	if len(p.Content) == 0 || json.Unmarshal(p.Content, &c) != nil {
		return ""
	}
	str := func(k string) string { s, _ := c[k].(string); return s }
	var s string
	switch p.Type {
	case "m.room.message":
		s = str("body")
	case "m.room.member":
		target := ""
		if p.StateKey != nil {
			target = *p.StateKey
		}
		s = str("membership") + " " + target
		if dn := str("displayname"); dn != "" {
			s += " (" + dn + ")"
		}
	case "m.room.name":
		s = str("name")
	case "m.room.topic":
		s = str("topic")
	case "m.room.canonical_alias":
		s = str("alias")
	case "m.room.create":
		s = "room version " + str("room_version")
		if s == "room version " {
			s = "room version 1"
		}
	case "m.room.join_rules":
		s = str("join_rule")
	case "m.room.history_visibility":
		s = str("history_visibility")
	case "m.reaction":
		if rel, ok := c["m.relates_to"].(map[string]any); ok {
			s, _ = rel["key"].(string)
		}
	case "m.room.redaction":
		s = "redacts " + p.Redacts
		if p.Redacts == "" {
			s = "redacts " + str("redacts")
		}
	default:
		if len(c) == 0 {
			return ""
		}
		if b := str("body"); b != "" {
			s = b
		}
	}
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > summaryLen {
		s = string([]rune(s)[:summaryLen]) + "…"
	}
	return s
}
