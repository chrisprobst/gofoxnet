package gofoxnet

import (
	"io"

	"github.com/chrisprobst/token"
)

type ThrottleOption func(*readWriteThrottle)

func ThrottleReading(bucket *token.Bucket) ThrottleOption {
	return func(t *readWriteThrottle) {
		t.readThrottle = bucket
	}
}

func ThrottleWriting(bucket *token.Bucket) ThrottleOption {
	return func(t *readWriteThrottle) {
		t.writeThrottle = bucket
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type readWriteThrottle struct {
	readThrottle  *token.Bucket
	writeThrottle *token.Bucket
}

type throttledReadWriteCloser struct {
	io.Reader
	io.Writer
	io.Closer
}

func (t *readWriteThrottle) setup(throttleOptions ...ThrottleOption) {
	for _, f := range throttleOptions {
		f(t)
	}
}

func (t *readWriteThrottle) throttle(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	trwc := &throttledReadWriteCloser{rwc, rwc, rwc}
	if t.readThrottle != nil {
		trwc.Reader = token.NewReader(t.readThrottle.View(), trwc.Reader)
	}
	if t.writeThrottle != nil {
		trwc.Writer = token.NewWriter(t.writeThrottle.View(), trwc.Writer)
	}
	return trwc
}

func (t *readWriteThrottle) done() {
	if t.readThrottle != nil {
		t.readThrottle.Done()
	}
	if t.writeThrottle != nil {
		t.writeThrottle.Done()
	}
}
