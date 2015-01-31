package gofoxnet

import "io"

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type Distributor struct {
	database  *database
	collector *collector
	forwarder *forwarder
	receiver  *receiver
}

func NewDistributor(rwc io.ReadWriteCloser) *Distributor {
	db := newDatabase()
	fw := newForwarder()
	return &Distributor{db, newCollector(db), fw, newReceiver(rwc, db, fw)}
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
