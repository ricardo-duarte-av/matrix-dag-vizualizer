package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/synapseadmin"
)

// CheckAdmin probes whether the account is a Synapse server admin. Any error
// (403, 404 on non-Synapse servers, network) is treated as "not admin".
func (c *Client) CheckAdmin(ctx context.Context) bool {
	var resp struct {
		Admin bool `json:"admin"`
	}
	url := c.cli.BuildURL(mautrix.SynapseAdminURLPath{"v1", "users", c.cli.UserID, "admin"})
	err := c.get(ctx, url, &resp)
	isAdmin := err == nil && resp.Admin
	if was := c.admin.Swap(isAdmin); was != isAdmin || err != nil {
		ev := c.log.Info().Bool("synapse_admin", isAdmin)
		if err != nil {
			ev = ev.AnErr("reason", err)
		}
		ev.Msg("Synapse admin status")
	}
	return isAdmin
}

func (c *Client) adminLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.AdminCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.CheckAdmin(ctx)
		}
	}
}

func (c *Client) adminClient() *synapseadmin.Client { return &synapseadmin.Client{Client: c.cli} }

// FetchEvent returns the full PDU of any event via the Synapse admin API.
func (c *Client) FetchEvent(ctx context.Context, eventID string) (json.RawMessage, error) {
	var resp struct {
		Event json.RawMessage `json:"event"`
	}
	err := c.get(ctx, c.cli.BuildURL(mautrix.SynapseAdminURLPath{"v1", "fetch_event", eventID}), &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Event) == 0 {
		return nil, fmt.Errorf("empty fetch_event response")
	}
	return resp.Event, nil
}

// Extremities returns the server's forward extremities of a room.
func (c *Client) Extremities(ctx context.Context, roomID string) ([]string, error) {
	var resp struct {
		Results []struct {
			EventID string `json:"event_id"`
		} `json:"results"`
	}
	err := c.get(ctx, c.cli.BuildURL(mautrix.SynapseAdminURLPath{"v1", "rooms", roomID, "forward_extremities"}), &resp)
	if err != nil {
		if isForbidden(err) {
			c.admin.Store(false)
		}
		return nil, err
	}
	out := make([]string, len(resp.Results))
	for i, r := range resp.Results {
		out[i] = r.EventID
	}
	return out, nil
}

// ServerRooms lists rooms known to the homeserver (Synapse admin only).
func (c *Client) ServerRooms(ctx context.Context, search string, from, limit int) (synapseadmin.RespListRooms, error) {
	resp, err := c.adminClient().ListRooms(ctx, synapseadmin.ReqListRoom{
		SearchTerm: search,
		OrderBy:    "joined_local_members",
		From:       from,
		Limit:      limit,
	})
	if err != nil && isForbidden(err) {
		c.admin.Store(false)
	}
	return resp, err
}

func (c *Client) adminRoomInfo(ctx context.Context, roomID string) (*synapseadmin.RoomInfo, error) {
	return c.adminClient().RoomInfo(ctx, id.RoomID(roomID))
}
