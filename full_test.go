package gofoxnet

import (
	"bytes"
	"net"
	"testing"
)

func TestFull(t *testing.T) {
	i := newInserter()

	// Create pipes for distributors
	id1, di1 := net.Pipe()
	id2, di2 := net.Pipe()
	id3, di3 := net.Pipe()

	// Add inserter peers
	i.addPeer(id1)
	i.addPeer(id2)
	i.addPeer(id3)

	// Create distributors
	dists := []*distributor{
		newDistributor(di1),
		newDistributor(di2),
		newDistributor(di3),
	}

	// Interconnect all peers
	for _, from := range dists {
		for _, to := range dists {
			if from != to {
				a, b := net.Pipe()
				from.collector.addPeer(a)
				to.forwarder.addPeer(b)
			}
		}
	}

	// The buffer for testing
	buffer := []byte("helloworldworks")
	h := hash(buffer)

	// Do the insertion
	i.insert(buffer)

	// Lookup the buffer on each peer
	for i, d := range dists {
		b, err := d.database.lookup(h)
		if err != nil {
			t.Fatal("Lookup of peer", i, "failed, Reason:", err)
		}

		if !bytes.Equal(b, buffer) {
			t.Fatal("Peer", i, "has unequal buffer content:", string(b), "!=", string(buffer))
		}
	}
}
