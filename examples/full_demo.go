package main

import (
	"bytes"
	"log"
	"net"

	"github.com/chrisprobst/gofoxnet"
	"github.com/chrisprobst/token"
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

	b := token.NewBucket(1*KiloByte, 32*Byte)
	di1 = token.NewReadConn(b.View(), di1)
	di2 = token.NewReadConn(b.View(), di2)
	di3 = token.NewReadConn(b.View(), di3)

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
	buckets := []*token.Bucket{
		token.NewBucket(1*KiloByte, 32*Byte),
		token.NewBucket(1*KiloByte, 32*Byte),
		token.NewBucket(1*KiloByte, 32*Byte),
	}

	// Interconnect all peers
	for i, from := range dists {
		for j, to := range dists {
			if from != to {
				a, b := net.Pipe()
				a = token.NewReadWriteConn(buckets[i].View(), a)
				b = token.NewReadWriteConn(buckets[j].View(), b)
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
}
