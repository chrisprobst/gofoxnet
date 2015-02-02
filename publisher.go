package gofoxnet

import "io"

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type Publisher struct {
	readWriteThrottle
	inserter *inserter
}

func NewPublisher(throttleOptions ...ThrottleOption) *Publisher {
	p := &Publisher{inserter: newInserter()}
	p.readWriteThrottle.setup(throttleOptions...)
	return p
}

func (p *Publisher) Close() error {
	err := p.inserter.closeAndWait()
	p.readWriteThrottle.done()
	return err
}

func (p *Publisher) AddPeer(rwc io.ReadWriteCloser) {
	p.inserter.addPeer(p.readWriteThrottle.throttle(rwc))
}

func (p *Publisher) Publish(buffer []byte) {
	p.inserter.insert(buffer)
}
