package p2p

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestForwarder(t *testing.T) {
	f := newForwarder()

	// Insert all peers
	peers := []*rwcBuffer{newRWCBuffer(), newRWCBuffer(), newRWCBuffer()}
	f.addPeer <- peers[0]
	f.addPeer <- peers[1]
	f.addPeer <- peers[2]

	// The buffer for testing
	packet := forwardingPacket{[]byte("HelloWorldHello"), 99}

	// Do the forwarding
	f.Forward(packet)

	// Recreate buffer
	var resultPacket1 forwardingPacket
	decoder := json.NewDecoder(peers[0].buffer)
	decoder.Decode(&resultPacket1)

	var resultPacket2 forwardingPacket
	decoder = json.NewDecoder(peers[1].buffer)
	decoder.Decode(&resultPacket2)

	var resultPacket3 forwardingPacket
	decoder = json.NewDecoder(peers[2].buffer)
	decoder.Decode(&resultPacket3)

	// Compare buffers
	if !bytes.Equal(resultPacket1.Buffer, resultPacket2.Buffer) {
		t.Fatal("Buffer 1 and 2 not equal. ", string(resultPacket1.Buffer), "!=", string(resultPacket2.Buffer))
	}
	if !bytes.Equal(resultPacket2.Buffer, resultPacket3.Buffer) {
		t.Fatal("Buffer 2 and 3 not equal. ", string(resultPacket2.Buffer), "!=", string(resultPacket3.Buffer))
	}

	// Close and wait
	f.CloseAndWait()
	if len(f.peers) != 0 {
		t.Fatal("Not all peers closed")
	}
}
