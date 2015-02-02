package gofoxnet

import (
	"bytes"
	"io"
	"log"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type insertionPacket struct {
	Hash        string
	SplitHashes []string
	Buffer      []byte
	BufferIndex int
}

func (p *insertionPacket) compatible(o *insertionPacket) bool {
	if p.Hash != o.Hash {
		return false
	}

	if len(p.SplitHashes) != len(o.SplitHashes) {
		return false
	}

	for i := 0; i < len(p.SplitHashes); i++ {
		if p.SplitHashes[i] != o.SplitHashes[i] {
			return false
		}
	}

	return true
}

func (p *insertionPacket) equals(o *insertionPacket) bool {
	return p.compatible(o) && p.BufferIndex == o.BufferIndex && bytes.Equal(p.Buffer, o.Buffer)
}

type distributorMetaInfo struct {
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type insertionPeerId uint64

type insertion struct {
	buffer []byte
	ready  signalChan
}

type insertionResult struct {
	id          insertionPeerId
	bufferIndex int
	err         error
}

type insertionPeerMetaInfo struct {
	distributorMetaInfo
	id insertionPeerId
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type insertionPeer struct {
	io.ReadWriteCloser

	// The unique id of this peer
	id insertionPeerId

	// An insertion chan, from which we get
	// net chunks to distribute
	insertionChan chan insertionPacket

	// The inserter, which created us
	inserter *inserter
}

func newInsertionPeer(rwc io.ReadWriteCloser, id insertionPeerId, inserter *inserter) *insertionPeer {
	p := &insertionPeer{rwc, id, make(chan insertionPacket), inserter}
	go p.processOutput()
	go p.processInput()
	return p
}

func (p *insertionPeer) processInput() {
	// Kill this peer if we are done
	defer p.inserter.kill(p.id)

	// Setup a new decoder
	decoder := msgpack.NewDecoder(p)

	for {
		// Try to decode meta info
		var mi distributorMetaInfo
		if err := decoder.Decode(&mi); err != nil {
			if err != io.EOF && err != io.ErrClosedPipe {
				log.Println(err)
			}
			break
		}

		// Finally update the inserter
		p.inserter.updateMetaInfo(insertionPeerMetaInfo{mi, p.id})
	}
}

func (p *insertionPeer) processOutput() {
	// Setup a new encoder
	encoder := msgpack.NewEncoder(p)

	// Receive new packets to write
	for packet := range p.insertionChan {

		// Try to encode to remote peer
		err := encoder.Encode(&packet)

		// We HAVE to answer for this packet
		p.inserter.addResult(insertionResult{p.id, packet.BufferIndex, err})
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type inserter struct {
	// Used to create ids
	nextPeerId insertionPeerId

	// A map storing all active peers
	peers map[insertionPeerId]*insertionPeer

	// New peers are inserted with this channel
	addPeerChan chan io.ReadWriteCloser

	// Kill requests are coming in on this channel
	killChan chan insertionPeerId

	// Meta info chan received from peers
	metaInfoChan chan insertionPeerMetaInfo

	// Used to insert bytes
	insertionChan chan insertion

	// Insertion result channel
	resultChan chan insertionResult

	// Used to schedule the close of this inserter
	done signalChan

	// Notified if closed
	closed signalChan
}

func newInserter() *inserter {
	i := &inserter{
		0,
		make(map[insertionPeerId]*insertionPeer),
		make(chan io.ReadWriteCloser),
		make(chan insertionPeerId),
		make(chan insertionPeerMetaInfo),
		make(chan insertion),
		make(chan insertionResult),
		make(signalChan),
		make(signalChan),
	}
	go i.serve()
	return i
}

func (i *inserter) removeAndClosePeer(id insertionPeerId) {
	if p, ok := i.peers[id]; ok {
		delete(i.peers, id)
		close(p.insertionChan)
		p.Close()
	}
}

func (i *inserter) createPeer(rwc io.ReadWriteCloser) {
	i.peers[i.nextPeerId] = newInsertionPeer(rwc, i.nextPeerId, i)
	i.nextPeerId++
}

func (i *inserter) updateMetaInfo(metaInfo insertionPeerMetaInfo) {
	select {
	case i.metaInfoChan <- metaInfo:
	case <-i.done:
		break
	}
}

func (i *inserter) addResult(result insertionResult) {
	select {
	case i.resultChan <- result:
	case <-i.done:
		break
	}
}

func (i *inserter) addPeer(rwc io.ReadWriteCloser) {
	select {
	case i.addPeerChan <- rwc:
	case <-i.done:
		break
	}
}

func (i *inserter) kill(id insertionPeerId) {
	select {
	case i.killChan <- id:
	case <-i.done:
		break
	}
}

func (i *inserter) processInsert(ins insertion) {
	// Close ready channel
	defer close(ins.ready)

	// Collect variables necessary for inserting
	buffer := ins.buffer
	count := len(i.peers)
	hash := Hash(buffer)
	splitHashes, splitBuffers := SplitAndHash(buffer, count)
	var notInserted []int

	bufferIndex := 0
	for _, c := range i.peers {
		// Create insertion packet for peer
		p := insertionPacket{
			hash,
			splitHashes,
			splitBuffers[bufferIndex],
			bufferIndex,
		}

		// Insert every packet,
		// If done, return!
		select {
		case c.insertionChan <- p:
		case <-i.done:
			log.Println("Inserter closed while inserting")
			return
		}

		bufferIndex++
	}

	// Register failed buffers
	for j := 0; j < count; j++ {
		select {
		case res := <-i.resultChan:
			if res.err != nil {
				// Remember failed chunks
				notInserted = append(notInserted, res.bufferIndex)

				// We can safely remove and close the peer here
				i.removeAndClosePeer(res.id)
			}
		case <-i.done:
			log.Println("Inserter closed while waiting for results")
			return
		}
	}

	if len(notInserted) > 0 {
		panic("Not all buffers inserted, do more about this")
	}
}

func (i *inserter) serve() {
	defer close(i.closed)

	// Select for adding, killing and inserting
	for running := true; running; {
		select {
		case rwc := <-i.addPeerChan:
			i.createPeer(rwc)
		case id := <-i.killChan:
			i.removeAndClosePeer(id)
		case info := <-i.metaInfoChan:
			log.Println(info)
		case insertion := <-i.insertionChan:
			i.processInsert(insertion)
		case <-i.done:
			running = false
			continue
		}
	}

	// Remove and close all peers
	for id := range i.peers {
		i.removeAndClosePeer(id)
	}
}

func (i *inserter) insert(buffer []byte) {
	ins := insertion{buffer, make(signalChan)}

	select {
	case i.insertionChan <- ins:
	case <-i.done:
		break
	}

	select {
	case <-ins.ready:
	case <-i.done:
	}
}

func (i *inserter) close() error {
	close(i.done)
	return nil
}

func (i *inserter) closeAndWait() error {
	err := i.close()
	<-i.closed
	return err
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type receiver struct {
	rwc       io.ReadWriteCloser
	database  *database
	forwarder *forwarder
}

func newReceiver(rwc io.ReadWriteCloser, database *database, forwarder *forwarder) *receiver {
	r := &receiver{rwc, database, forwarder}
	go r.processInput()
	return r
}

func (r *receiver) processInput() {
	defer r.close()

	// Setup a new decoder
	decoder := msgpack.NewDecoder(r.rwc)

	for {

		// Try to decode insertion packet
		var ip insertionPacket
		if err := decoder.Decode(&ip); err != nil {
			if err != io.EOF && err != io.ErrClosedPipe {
				log.Println(err)
			}
			break
		}

		// Insert meta data and chunk into database
		r.database.addMetaData(metadata{ip.Hash, ip.SplitHashes})
		r.database.addChunk(chunk{ip.Hash, ip.Buffer, ip.BufferIndex})
		r.forwarder.forward(forwardingPacket{ip.Hash, ip.Buffer, ip.BufferIndex})
	}
}

func (r *receiver) close() error {
	return r.rwc.Close()
}
