package gofoxnet

import (
	"io"

	"github.com/chrisprobst/token"
)

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func NewPublisher() *Publisher {
	return NewThrottledPublisher(nil)
}

func NewThrottledPublisher(bucket *token.Bucket) *Publisher {
	return &Publisher{newInserter(), bucket}
}

type Publisher struct {
	inserter *inserter
	bucket   *token.Bucket
}

func (p *Publisher) Close() error {
	err := p.inserter.closeAndWait()
	if p.bucket != nil {
		p.bucket.Done()
	}
	return err
}

func (p *Publisher) AddPeer(rwc io.ReadWriteCloser) {
	if p.bucket != nil {
		rwc = token.NewReadWriteCloser(p.bucket.View(), rwc)
	}
	p.inserter.addPeer(rwc)
}

func (p *Publisher) Publish(buffer []byte) {
	p.inserter.insert(buffer)
}
