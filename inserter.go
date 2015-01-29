package gofoxnet

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
)

type insertionPeerId uint64

type insertion struct {
	buffer []byte
	ready  signalChan
}

type insertionPacket struct {
	Hash        string   `json:"hash"`
	SplitHashes []string `json:"splitHashes"`
	Buffer      []byte   `json:"buffer"`
	BufferIndex int      `json:"bufferIndex"`
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

type insertionResult struct {
	id          insertionPeerId
	bufferIndex int
	err         error
}

type distributorMetaInfo struct {
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
	inserter *Inserter
}

func newInsertionPeer(rwc io.ReadWriteCloser, id insertionPeerId, inserter *Inserter) *insertionPeer {
	p := &insertionPeer{rwc, id, make(chan insertionPacket), inserter}
	go p.processOutput()
	go p.processInput()
	return p
}

func (p *insertionPeer) processInput() {
	// Kill this peer if we are done
	defer p.inserter.kill(p.id)

	// Setup a new decoder
	decoder := json.NewDecoder(p)

	for {
		// Try to decode meta info
		var mi distributorMetaInfo
		if err := decoder.Decode(&mi); err != nil {
			if err != io.EOF {
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
	encoder := json.NewEncoder(p)

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

type Inserter struct {
	// Used to create ids
	nextPeerId insertionPeerId

	// A map storing all active peers
	peers map[insertionPeerId]*insertionPeer

	// New peers are inserted with this channel
	addPeer chan io.ReadWriteCloser

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

func (i *Inserter) removeAndClosePeer(id insertionPeerId) {
	if p, ok := i.peers[id]; ok {
		delete(i.peers, id)
		close(p.insertionChan)
		p.Close()
	}
}

func (i *Inserter) createPeer(rwc io.ReadWriteCloser) {
	i.peers[i.nextPeerId] = newInsertionPeer(rwc, i.nextPeerId, i)
	i.nextPeerId++
}

func (i *Inserter) updateMetaInfo(metaInfo insertionPeerMetaInfo) {
	select {
	case i.metaInfoChan <- metaInfo:
	case <-i.done:
		break
	}
}

func (i *Inserter) addResult(result insertionResult) {
	select {
	case i.resultChan <- result:
	case <-i.done:
		break
	}
}

func (i *Inserter) kill(id insertionPeerId) {
	select {
	case i.killChan <- id:
	case <-i.done:
		break
	}
}

func (i *Inserter) processInsert(ins insertion) {
	// Close ready channel
	defer close(ins.ready)

	// Collect variables necessary for inserting
	buffer := ins.buffer
	count := len(i.peers)
	hash := hash(buffer)
	splitHashes, splitBuffers := splitAndHash(buffer, count)
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

func (i *Inserter) serve() {
	// Select for adding, killing and inserting
	for running := true; running; {
		select {
		case rwc := <-i.addPeer:
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

	close(i.closed)
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func NewInserter() *Inserter {
	i := &Inserter{
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

func (i *Inserter) Insert(buffer []byte) {
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

func (i *Inserter) Close() error {
	close(i.done)
	return nil
}

func (i *Inserter) CloseAndWait() error {
	err := i.Close()
	<-i.closed
	return err
}
