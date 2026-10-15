//go:build goexperiment.jsonv2 || go1.27

package matrix

import (
	"encoding/json/v2"

	"maunium.net/go/mautrix/federation/pdu"
	"maunium.net/go/mautrix/id"
)

const eventIDsVerifiable = true

// computeEventID derives an event's ID from its content (its reference hash
// for room v3+), so a remote server can't pass off a different event.
func computeEventID(raw []byte, roomVersion string) (string, error) {
	rv := id.RoomVersion(roomVersion)
	if !rv.IsKnown() {
		return "", errUnverifiable
	}
	var evt pdu.AnyPDU
	if rv.EventIDFormat() == id.EventIDFormatCustom {
		evt = &pdu.RoomV1PDU{}
	} else {
		evt = &pdu.PDU{}
	}
	if err := json.Unmarshal(raw, evt); err != nil {
		return "", err
	}
	eventID, err := evt.GetEventID(rv)
	return string(eventID), err
}
