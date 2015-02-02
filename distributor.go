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
	database         *database
	collector        *collector
	forwarder        *forwarder
	receiver         *receiver
	uploadThrottle   *token.Bucket
	downloadThrottle *token.Bucket
}

func NewDistributor(rwc io.ReadWriteCloser) *Distributor {
	db := newDatabase()
	fw := newForwarder()
	return &Distributor{db, newCollector(db), fw, newReceiver(rwc, db, fw), nil, nil}
}

func (d *Distributor) AddCollectorPeer(rwc io.ReadWriteCloser) {
	d.collector.addPeer(rwc)
}

func (d *Distributor) AddForwardingPeer(rwc io.ReadWriteCloser) {
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
	return errors.Error()
}
