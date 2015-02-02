package main

import (
	"bytes"
	"log"
	"net"

	"github.com/chrisprobst/gofoxnet"
)

const (
	Byte     = 1
	KiloByte = 1024 * Byte
	MegaByte = 1024 * KiloByte
)

func main() {

	p := gofoxnet.NewPublisher()

	// Create pipes for distributors
	id1, di1 := net.Pipe()
	id2, di2 := net.Pipe()
	id3, di3 := net.Pipe()

	// Add inserter peers
	p.AddPeer(id1)
	p.AddPeer(id2)
	p.AddPeer(id3)

	// Create distributors
	dists := []*gofoxnet.Distributor{
		gofoxnet.NewDistributor(di1),
		gofoxnet.NewDistributor(di2),
		gofoxnet.NewDistributor(di3),
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
	h := gofoxnet.Hash(buffer)

	// Do the insertion
	p.Publish(buffer)

	// Lookup the buffer on each peer
	for i, d := range dists {
		b, err := d.Lookup(h)
		if err != nil {
			log.Fatal("Lookup of peer", i, "failed, Reason:", err)
		}

		if !bytes.Equal(b, buffer) {
			log.Fatal("Peer", i, "has unequal buffer content:", string(b), "!=", string(buffer))
		}
	}

	if err := p.Close(); err != nil {
		log.Fatal("Failed to close publisher:", err)
	}

	for i, d := range dists {
		if err := d.Close(); err != nil {
			log.Fatal("Failed to close distributor no. ", i, ":", err)
		}
	}
}
