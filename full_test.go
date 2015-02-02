package gofoxnet

import (
	"bytes"
	"net"
	"testing"
)

func TestFull(t *testing.T) {
	p := NewPublisher()

	// Create pipes for distributors
	id1, di1 := net.Pipe()
	id2, di2 := net.Pipe()
	id3, di3 := net.Pipe()

	// Add inserter peers
	p.AddPeer(id1)
	p.AddPeer(id2)
	p.AddPeer(id3)

	// Create distributors
	dists := []*Distributor{
		NewDistributor(di1),
		NewDistributor(di2),
		NewDistributor(di3),
	}

	// Interconnect all peers
	for _, from := range dists {
		for _, to := range dists {
			if from != to {
				a, b := net.Pipe()
				from.AddCollectorPeer(a)
				to.AddForwardingPeer(b)
			}
		}
	}

	// The buffer for testing
	buffer := []byte("helloworldworks")
	h := Hash(buffer)

	// Do the insertion
	p.Publish(buffer)

	// Lookup the buffer on each peer
	for i, d := range dists {
		b, err := d.Lookup(h)
		if err != nil {
			t.Fatal("Lookup of peer", i, "failed, Reason:", err)
		}

		if !bytes.Equal(b, buffer) {
			t.Fatal("Peer", i, "has unequal buffer content:", string(b), "!=", string(buffer))
		}
	}

	if err := p.Close(); err != nil {
		t.Fatal("Failed to close publisher:", err)
	}

	for i, d := range dists {
		if err := d.Close(); err != nil {
			t.Fatal("Failed to close distributor no. ", i, ":", err)
		}
	}
}
