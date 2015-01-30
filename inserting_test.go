package gofoxnet

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestInserter(t *testing.T) {
	i := newInserter()

	// Insert all peers
	peers := []*rwcBuffer{newRWCBuffer(), newRWCBuffer(), newRWCBuffer()}
	i.addPeer(peers[0])
	i.addPeer(peers[1])
	i.addPeer(peers[2])

	// The buffer for testing
	buffer := []byte("HelloWorldHello")

	// Do the insertion
	i.insert(buffer)

	// Recreate buffer
	resultBuffer := make([]byte, 15)
	var packet, packet2 insertionPacket

	decoder := json.NewDecoder(peers[0].buffer)
	decoder.Decode(&packet)
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)

	decoder = json.NewDecoder(peers[1].buffer)
	decoder.Decode(&packet2)
	if !packet.compatible(&packet2) {
		t.Fatal("Packet 1 and 2 not compatible:", packet, "!=", packet2)
	}
	copy(resultBuffer[packet2.BufferIndex*5:packet2.BufferIndex*5+5], packet2.Buffer)

	decoder = json.NewDecoder(peers[2].buffer)
	decoder.Decode(&packet)
	if !packet2.compatible(&packet) {
		t.Fatal("Packet 2 and 3 not compatible:", packet2, "!=", packet)
	}
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)

	// Compare buffers
	if !bytes.Equal(buffer, resultBuffer) {
		t.Fatal("Buffer and result buffer not equal:", string(buffer), "!=", string(resultBuffer))
	}

	// Close and wait
	i.closeAndWait()
	if len(i.peers) != 0 {
		t.Fatal("Not all peers closed")
	}
}
