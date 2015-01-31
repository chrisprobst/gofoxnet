package gofoxnet

import (
	"bytes"
	"testing"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestForwarder(t *testing.T) {
	f := newForwarder()

	// Insert all peers
	peers := []*rwcBuffer{newRWCBuffer(), newRWCBuffer(), newRWCBuffer()}
	f.addPeer(peers[0])
	f.addPeer(peers[1])
	f.addPeer(peers[2])

	// The buffer for testing
	packet := forwardingPacket{"#hashtag", []byte("HelloWorldHello"), 99}

	// Do the forwarding
	f.forward(packet)

	// Recreate buffer
	var resultPacket1 forwardingPacket
	decoder := msgpack.NewDecoder(peers[0].buffer)
	decoder.Decode(&resultPacket1)

	var resultPacket2 forwardingPacket
	decoder = msgpack.NewDecoder(peers[1].buffer)
	decoder.Decode(&resultPacket2)

	var resultPacket3 forwardingPacket
	decoder = msgpack.NewDecoder(peers[2].buffer)
	decoder.Decode(&resultPacket3)

	// Compare buffers
	if !resultPacket1.equals(&resultPacket2) {
		t.Fatal("Packet 1 and 2 not equal:", resultPacket1, "!=", resultPacket2)
	}
	if !resultPacket2.equals(&resultPacket3) {
		t.Fatal("Packet 2 and 3 not equal:", resultPacket2, "!=", resultPacket3)
	}

	// Close and wait
	f.closeAndWait()
	if len(f.peers) != 0 {
		t.Fatal("Not all peers closed")
	}
}

func TestCollector(t *testing.T) {
	d := newDatabase()
	c := newCollector(d)

	// Fake packets and readers
	data := []byte("helloworldworks")
	h := Hash(data)
	f1 := forwardingPacket{h, []byte("hello"), 0}
	b, _ := msgpack.Marshal(f1)
	r1 := bytes.NewReader(b)

	f2 := forwardingPacket{h, []byte("world"), 1}
	b, _ = msgpack.Marshal(f2)
	r2 := bytes.NewReader(b)

	f3 := forwardingPacket{h, []byte("works"), 2}
	b, _ = msgpack.Marshal(f3)
	r3 := bytes.NewReader(b)

	// Register peers
	c.addPeer(newRWC(r1))
	c.addPeer(newRWC(r2))
	c.addPeer(newRWC(r3))

	// Make sure the packets,
	// which are received,
	// make any sense
	d.addMetaData(metadata{
		h,
		[]string{
			Hash([]byte("hello")),
			Hash([]byte("world")),
			Hash([]byte("works")),
		},
	})

	// Wait for database lookup
	b, err := d.lookup(h)

	if err != nil {
		t.Fatal("Database lookup failed:", err)
	}

	if !bytes.Equal(b, data) {
		t.Fatal("Forwarded buffer does not equal database buffer", string(b), "!=", string(data))
	}

	// Close and wait
	c.closeAndWait()
	if len(c.peers) != 0 {
		t.Fatal("Not all peers closed")
	}
}
