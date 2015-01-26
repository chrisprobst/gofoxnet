package p2p

import (
	"bytes"
	"io"
	"sync"
)

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type rwcBuffer struct {
	buffer *bytes.Buffer
	closed bool
	mutex  sync.Mutex
	cond   *sync.Cond
}

func (r *rwcBuffer) Write(buffer []byte) (int, error) {
	return r.buffer.Write(buffer)
}

func (r *rwcBuffer) Read(buffer []byte) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for !r.closed {
		r.cond.Wait()
	}
	return 0, io.EOF
}

func (r *rwcBuffer) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.closed = true
	r.cond.Broadcast()
	return nil
}

func newRWCBuffer() *rwcBuffer {
	b := &rwcBuffer{buffer: new(bytes.Buffer)}
	b.cond = sync.NewCond(&b.mutex)
	return b
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////