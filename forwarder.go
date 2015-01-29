package p2p

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
)

type forwardingPeerId uint64

type forwarding struct {
	packet forwardingPacket
	ready  signalChan
}

type forwardingPacket struct {
	Hash        string `json:"hash"`
	Buffer      []byte `json:"buffer"`
	BufferIndex int    `json:"bufferIndex"`
}

func (p *forwardingPacket) compatible(o *forwardingPacket) bool {
	return p.Hash == o.Hash
}

func (p *forwardingPacket) equals(o *forwardingPacket) bool {
	return p.compatible(o) && p.BufferIndex == o.BufferIndex && bytes.Equal(p.Buffer, o.Buffer)
}

type forwardingResult struct {
	id  forwardingPeerId
	err error
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type forwardingPeer struct {
	io.ReadWriteCloser

	// The unique id of this peer
	id forwardingPeerId

	// An forwarding chan, from which we get
	// net chunks to forward
	forwardingChan chan forwardingPacket

	// The forwarder, which created us
	forwarder *Forwarder
}

func newForwardingPeer(rwc io.ReadWriteCloser, id forwardingPeerId, forwarder *Forwarder) *forwardingPeer {
	p := &forwardingPeer{rwc, id, make(chan forwardingPacket), forwarder}
	go p.processOutput()
	return p
}

func (p *forwardingPeer) processOutput() {
	// Setup a new encoder
	encoder := json.NewEncoder(p)

	// Receive new packets to write
	for packet := range p.forwardingChan {

		// Try to encode to remote peer
		err := encoder.Encode(&packet)

		// We HAVE to answer for this packet
		p.forwarder.addResult(forwardingResult{p.id, err})
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type Forwarder struct {
	// Used to create ids
	nextPeerId forwardingPeerId

	// A map storing all active peers
	peers map[forwardingPeerId]*forwardingPeer

	// New peers are inserted with this channel
	addPeer chan io.ReadWriteCloser

	// Kill requests are coming in on this channel
	killChan chan forwardingPeerId

	// Used to forward packets
	forwardingChan chan forwarding

	// Forwarding result channel
	resultChan chan forwardingResult

	// Used to schedule the close of this forwarder
	done signalChan

	// Notified if closed
	closed signalChan
}

func (f *Forwarder) removeAndClosePeer(id forwardingPeerId) {
	if p, ok := f.peers[id]; ok {
		delete(f.peers, id)
		close(p.forwardingChan)
		p.Close()
	}
}

func (f *Forwarder) createPeer(rwc io.ReadWriteCloser) {
	f.peers[f.nextPeerId] = newForwardingPeer(rwc, f.nextPeerId, f)
	f.nextPeerId++
}

func (f *Forwarder) addResult(result forwardingResult) {
	select {
	case f.resultChan <- result:
	case <-f.done:
		break
	}
}

func (f *Forwarder) kill(id forwardingPeerId) {
	select {
	case f.killChan <- id:
	case <-f.done:
		break
	}
}

func (f *Forwarder) processForwarding(forwarding forwarding) {
	// Close ready channel
	defer close(forwarding.ready)

	// Collect variables necessary for forwarding
	count := len(f.peers)
	var notForwarded []forwardingPeerId

	for _, c := range f.peers {
		// Forward every packet,
		// If done, return!
		select {
		case c.forwardingChan <- forwarding.packet:
		case <-f.done:
			log.Println("Forwarder closed while inserting")
			return
		}
	}

	// Register failed packets
	for j := 0; j < count; j++ {
		select {
		case res := <-f.resultChan:
			if res.err != nil {
				// Remember failed packets
				notForwarded = append(notForwarded, res.id)

				// We can safely remove and close the peer here
				f.removeAndClosePeer(res.id)
			}
		case <-f.done:
			log.Println("Forwarder closed while waiting for results")
			return
		}
	}

	if len(notForwarded) > 0 {
		panic("Not all buffers forwarded, do more about this")
	}
}

func (f *Forwarder) serve() {
	// Select for adding, killing and forwarding
	for running := true; running; {
		select {
		case rwc := <-f.addPeer:
			f.createPeer(rwc)
		case id := <-f.killChan:
			f.removeAndClosePeer(id)
		case forwarding := <-f.forwardingChan:
			f.processForwarding(forwarding)
		case <-f.done:
			running = false
			continue
		}
	}

	// Remove and close all peers
	for id := range f.peers {
		f.removeAndClosePeer(id)
	}

	close(f.closed)
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func newForwarder() *Forwarder {
	f := &Forwarder{
		0,
		make(map[forwardingPeerId]*forwardingPeer),
		make(chan io.ReadWriteCloser),
		make(chan forwardingPeerId),
		make(chan forwarding),
		make(chan forwardingResult),
		make(signalChan),
		make(signalChan),
	}
	go f.serve()
	return f
}

func (f *Forwarder) Forward(packet forwardingPacket) {
	forwarding := forwarding{packet, make(signalChan)}

	select {
	case f.forwardingChan <- forwarding:
	case <-f.done:
		break
	}

	select {
	case <-forwarding.ready:
	case <-f.done:
	}
}

func (f *Forwarder) Close() error {
	close(f.done)
	return nil
}

func (f *Forwarder) CloseAndWait() error {
	err := f.Close()
	<-f.closed
	return err
}
