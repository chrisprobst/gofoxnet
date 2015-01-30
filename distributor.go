package gofoxnet

import "io"

type distributor struct {
	database  *database
	collector *collector
	forwarder *forwarder
	receiver  *receiver
}

func newDistributor(rwc io.ReadWriteCloser) *distributor {
	db := newDatabase()
	fw := newForwarder()
	return &distributor{db, newCollector(db), fw, newReceiver(rwc, db, fw)}
}
