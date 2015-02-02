package gofoxnet

import (
	"io"

	"github.com/augustoroman/multierror"
)

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type Distributor struct {
	readWriteThrottle
	database  *database
	collector *collector
	forwarder *forwarder
	receiver  *receiver
}

func NewDistributor(rwc io.ReadWriteCloser, throttleOptions ...ThrottleOption) *Distributor {
	var d Distributor
	d.readWriteThrottle.setup(throttleOptions...)
	d.database = newDatabase()
	d.forwarder = newForwarder()
	d.collector = newCollector(d.database)
	d.receiver = newReceiver(d.readWriteThrottle.throttle(rwc), d.database, d.forwarder)
	return &d
}

func (d *Distributor) AddCollectorPeer(rwc io.ReadWriteCloser) {
	d.collector.addPeer(d.readWriteThrottle.throttle(rwc))
}

func (d *Distributor) AddForwardingPeer(rwc io.ReadWriteCloser) {
	d.forwarder.addPeer(d.readWriteThrottle.throttle(rwc))
}

func (d *Distributor) Lookup(hash string) ([]byte, error) {
	return d.database.lookup(hash)
}

func (d *Distributor) Close() error {
	var errors multierror.Accumulator
	errors.Push(d.receiver.close())
	errors.Push(d.forwarder.closeAndWait())
	errors.Push(d.collector.closeAndWait())
	errors.Push(d.database.closeAndWait())
	d.readWriteThrottle.done()
	return errors.Error()
}
