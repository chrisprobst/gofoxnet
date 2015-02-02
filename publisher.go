package gofoxnet

import (
	"io"

	"github.com/chrisprobst/token"
)

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func NewPublisher() *Publisher {
	return &Publisher{newInserter(), nil, nil}
}

type Publisher struct {
	inserter         *inserter
	uploadThrottle   *token.Bucket
	downloadThrottle *token.Bucket
}

func (p *Publisher) Close() error {
	err := p.inserter.closeAndWait()
	return err
}

func (p *Publisher) AddPeer(rwc io.ReadWriteCloser) {
	p.inserter.addPeer(rwc)
}

func (p *Publisher) Publish(buffer []byte) {
	p.inserter.insert(buffer)
}
