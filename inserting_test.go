package gofoxnet

import (
	"bytes"
	"net"
	"testing"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
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

	decoder := msgpack.NewDecoder(peers[0].buffer)
	decoder.Decode(&packet)
	copy(resultBuffer[packet.BufferIndex*5:packet.BufferIndex*5+5], packet.Buffer)

	decoder = msgpack.NewDecoder(peers[1].buffer)
	decoder.Decode(&packet2)
	if !packet.compatible(&packet2) {
		t.Fatal("Packet 1 and 2 not compatible:", packet, "!=", packet2)
	}
	copy(resultBuffer[packet2.BufferIndex*5:packet2.BufferIndex*5+5], packet2.Buffer)

	decoder = msgpack.NewDecoder(peers[2].buffer)
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

func TestReceiver(t *testing.T) {
	i := newInserter()

	// Create pipes for io
	l1, r1 := net.Pipe()
	l2, r2 := net.Pipe()
	l3, r3 := net.Pipe()

	// Create receivers
	rcv1 := newReceiver(r1, newDatabase(), newForwarder())
	rcv2 := newReceiver(r2, newDatabase(), newForwarder())
	rcv3 := newReceiver(r3, newDatabase(), newForwarder())

	// Add all peers
	i.addPeer(l1)
	i.addPeer(l2)
	i.addPeer(l3)

	// The buffer for testing
	buffer := []byte("helloworldworks")

	// Do the insertion
	i.insert(buffer)

	// Make sure each goroutine has read
	time.Sleep(time.Millisecond * 100)

	i.closeAndWait()
	if len(i.peers) != 0 {
		t.Fatal("Not all peers closed")
	}

	rcv1.close()
	rcv2.close()
	rcv3.close()
}
