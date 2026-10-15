//go:build !(goexperiment.jsonv2 || go1.27)

package matrix

// Without encoding/json/v2, mautrix's PDU hashing isn't available, so events
// fetched over federation are accepted after the room check only.

const eventIDsVerifiable = false

func computeEventID([]byte, string) (string, error) { return "", errUnverifiable }
