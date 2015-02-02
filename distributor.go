package gofoxnet

import (
	"io"

	"github.com/augustoroman/multierror"
	"github.com/chrisprobst/token"
)

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type Distributor struct {
	database  *database
	collector *collector
	forwarder *forwarder
	receiver  *receiver
	bucket    *token.Bucket
}

func NewDistributor(rwc io.ReadWriteCloser) *Distributor {
	return NewThrottledDistributor(rwc, nil)
}

func NewThrottledDistributor(rwc io.ReadWriteCloser, bucket *token.Bucket) *Distributor {
	db := newDatabase()
	fw := newForwarder()
	return &Distributor{db, newCollector(db), fw, newReceiver(rwc, db, fw), bucket}
}

func (d *Distributor) AddCollectorPeer(rwc io.ReadWriteCloser) {
	if d.bucket != nil {
		rwc = token.NewReadWriteCloser(d.bucket.View(), rwc)
	}
	d.collector.addPeer(rwc)
}

func (d *Distributor) AddForwardingPeer(rwc io.ReadWriteCloser) {
	if d.bucket != nil {
		rwc = token.NewReadWriteCloser(d.bucket.View(), rwc)
	}
	d.forwarder.addPeer(rwc)
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
	if d.bucket != nil {
		d.bucket.Done()
	}
	return errors.Error()
}
