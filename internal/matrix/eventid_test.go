//go:build goexperiment.jsonv2 || go1.27

package matrix

import (
	"os"
	"strings"
	"testing"
)

func TestComputeEventIDRoomV12(t *testing.T) {
	// An org.matrix.dummy_event from Matrix HQ, as returned by fetch_event.
	raw, err := os.ReadFile("testdata/v12_dummy_event.json")
	if err != nil {
		t.Fatal(err)
	}
	const want = "$liQuMwKvUU_2jVaTpTmDA-rK09YO7O_moFggAtdf428"
	got, err := computeEventID(raw, "12")
	if err != nil || got != want {
		t.Fatalf("computeEventID = %q, %v; want %q", got, err, want)
	}
	const room = "!L58ME6ufiP49v97UIOBIpvWKEgj4912JmECPuDzlvCI"
	if err := checkRemoteEvent(raw, room, want, "12"); err != nil {
		t.Fatalf("checkRemoteEvent rejected the genuine event: %v", err)
	}
	tampered := []byte(strings.Replace(string(raw), `"depth"`, `"depth_"`, 1))
	if err := checkRemoteEvent(tampered, room, want, "12"); err == nil {
		t.Fatal("checkRemoteEvent accepted a modified event")
	}
	if err := checkRemoteEvent(raw, "!other", want, "12"); err == nil {
		t.Fatal("checkRemoteEvent accepted an event from another room")
	}
}
