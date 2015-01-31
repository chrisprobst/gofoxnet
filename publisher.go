package gofoxnet

import "io"

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func NewPublisher() *Publisher {
	return &Publisher{newInserter()}
}

type Publisher struct {
	inserter *inserter
}

func (p *Publisher) Close() error {
	return p.inserter.closeAndWait()
}

func (p *Publisher) AddPeer(rwc io.ReadWriteCloser) {
	p.inserter.addPeer(rwc)
}

func (p *Publisher) Publish(buffer []byte) {
	p.inserter.insert(buffer)
}
