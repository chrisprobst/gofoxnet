package p2p

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestInserter(t *testing.T) {
	i := NewInserter()

	// Insert all peers
	peers := []*rwcBuffer{newRWCBuffer(), newRWCBuffer(), newRWCBuffer()}
	i.addPeer <- peers[0]
	i.addPeer <- peers[1]
	i.addPeer <- peers[2]

	// The buffer for testing
	buffer := []byte("HelloWorldHello")

	// Do the insertion
	i.Insert(buffer)

	// Recreate buffer
	resultBuffer := make([]byte, 15)
	var packet insertionPacket
	decoder := json.NewDecoder(peers[0].buffer)
	decoder.Decode(&packet)
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)
	decoder = json.NewDecoder(peers[1].buffer)
	decoder.Decode(&packet)
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)
	decoder = json.NewDecoder(peers[2].buffer)
	decoder.Decode(&packet)
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)

	// Compare buffers
	if !bytes.Equal(buffer, resultBuffer) {
		t.Fatal("Buffer and result buffer not equal. ", string(buffer), "!=", string(resultBuffer))
	}

	// Close and wait
	i.CloseAndWait()
	if len(i.peers) != 0 {
		t.Fatal("Not all peers closed")
	}
}
