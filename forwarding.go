package gofoxnet

import (
	"bytes"
	"io"
	"log"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type forwardingPacket struct {
	Hash        string
	Buffer      []byte
	BufferIndex int
}

func (p *forwardingPacket) compatible(o *forwardingPacket) bool {
	return p.Hash == o.Hash
}

func (p *forwardingPacket) equals(o *forwardingPacket) bool {
	return p.compatible(o) && p.BufferIndex == o.BufferIndex && bytes.Equal(p.Buffer, o.Buffer)
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type forwardingPeerId uint64

type forwarding struct {
	packet forwardingPacket
	ready  signalChan
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
	forwarder *forwarder
}

func newForwardingPeer(rwc io.ReadWriteCloser, id forwardingPeerId, forwarder *forwarder) *forwardingPeer {
	p := &forwardingPeer{rwc, id, make(chan forwardingPacket), forwarder}
	go p.processOutput()
	return p
}

func (p *forwardingPeer) processOutput() {
	// Setup a new encoder
	encoder := msgpack.NewEncoder(p)

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

type forwarder struct {
	// Used to create ids
	nextPeerId forwardingPeerId

	// A map storing all active peers
	peers map[forwardingPeerId]*forwardingPeer

	// New peers are inserted with this channel
	addPeerChan chan io.ReadWriteCloser

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

func newForwarder() *forwarder {
	f := &forwarder{
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

func (f *forwarder) removeAndClosePeer(id forwardingPeerId) {
	if p, ok := f.peers[id]; ok {
		delete(f.peers, id)
		close(p.forwardingChan)
		p.Close()
	}
}

func (f *forwarder) createPeer(rwc io.ReadWriteCloser) {
	f.peers[f.nextPeerId] = newForwardingPeer(rwc, f.nextPeerId, f)
	f.nextPeerId++
}

func (f *forwarder) addResult(result forwardingResult) {
	select {
	case f.resultChan <- result:
	case <-f.done:
		break
	}
}

func (f *forwarder) addPeer(rwc io.ReadWriteCloser) {
	select {
	case f.addPeerChan <- rwc:
	case <-f.done:
		break
	}
}

func (f *forwarder) kill(id forwardingPeerId) {
	select {
	case f.killChan <- id:
	case <-f.done:
		break
	}
}

func (f *forwarder) processForwarding(forwarding forwarding) {
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

func (f *forwarder) serve() {
	defer close(f.closed)

	// Select for adding, killing and forwarding
	for running := true; running; {
		select {
		case rwc := <-f.addPeerChan:
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
}

func (f *forwarder) forward(packet forwardingPacket) {
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

func (f *forwarder) close() error {
	close(f.done)
	return nil
}

func (f *forwarder) closeAndWait() error {
	err := f.close()
	<-f.closed
	return err
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type collectingPeerId uint64

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type collectingPeer struct {
	io.ReadWriteCloser

	// The unique id of this peer
	id collectingPeerId

	// The collector, which created us
	collector *collector
}

func newCollectingPeer(rwc io.ReadWriteCloser, id collectingPeerId, collector *collector) *collectingPeer {
	p := &collectingPeer{rwc, id, collector}
	go p.processInput()
	return p
}

func (p *collectingPeer) processInput() {
	// Kill this peer if we are done
	defer p.collector.kill(p.id)

	// Setup a new decoder
	decoder := msgpack.NewDecoder(p)

	for {
		// Try to decode forwarding packet
		var fp forwardingPacket
		if err := decoder.Decode(&fp); err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}

		// Finally push to collector
		p.collector.collect(fp)
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type collector struct {
	// Used to create ids
	nextPeerId collectingPeerId

	// A map storing all active peers
	peers map[collectingPeerId]*collectingPeer

	// New peers are inserted with this channel
	addPeerChan chan io.ReadWriteCloser

	// Kill requests are coming in on this channel
	killChan chan collectingPeerId

	// Used to collect forwarding packets
	packetChan chan forwardingPacket

	// The database to store the packets
	database *database

	// Used to schedule the close of this forwarder
	done signalChan

	// Notified if closed
	closed signalChan
}

func newCollector(database *database) *collector {
	c := &collector{
		0,
		make(map[collectingPeerId]*collectingPeer),
		make(chan io.ReadWriteCloser),
		make(chan collectingPeerId),
		make(chan forwardingPacket),
		database,
		make(signalChan),
		make(signalChan),
	}
	go c.serve()
	return c
}

func (c *collector) removeAndClosePeer(id collectingPeerId) {
	if p, ok := c.peers[id]; ok {
		delete(c.peers, id)
		p.Close()
	}
}

func (c *collector) createPeer(rwc io.ReadWriteCloser) {
	c.peers[c.nextPeerId] = newCollectingPeer(rwc, c.nextPeerId, c)
	c.nextPeerId++
}

func (c *collector) addPeer(rwc io.ReadWriteCloser) {
	select {
	case c.addPeerChan <- rwc:
	case <-c.done:
		break
	}
}

func (c *collector) kill(id collectingPeerId) {
	select {
	case c.killChan <- id:
	case <-c.done:
		break
	}
}

func (c *collector) collect(fp forwardingPacket) {
	select {
	case c.packetChan <- fp:
	case <-c.done:
		break
	}
}

func (c *collector) serve() {
	defer close(c.closed)

	// Select for adding, killing and collecting
	for running := true; running; {
		select {
		case rwc := <-c.addPeerChan:
			c.createPeer(rwc)
		case id := <-c.killChan:
			c.removeAndClosePeer(id)
		case fp := <-c.packetChan:
			c.database.addChunk(chunk{fp.Hash, fp.Buffer, fp.BufferIndex})
		case <-c.done:
			running = false
			continue
		}
	}

	// Remove and close all peers
	for id := range c.peers {
		c.removeAndClosePeer(id)
	}
}

func (c *collector) close() error {
	close(c.done)
	return nil
}

func (c *collector) closeAndWait() error {
	err := c.close()
	<-c.closed
	return err
}
