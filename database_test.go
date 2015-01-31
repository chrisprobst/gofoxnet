package gofoxnet

import (
	"bytes"
	"testing"
	"time"
)

func TestDatabase(t *testing.T) {
	d := newDatabase()
	h := Hash([]byte("helloworldworks"))
	c1 := chunk{h, []byte("hello"), 0}
	c2 := chunk{h, []byte("world"), 1}
	c3 := chunk{h, []byte("works"), 2}
	md := metadata{
		h,
		[]string{
			Hash([]byte("hello")),
			Hash([]byte("world")),
			Hash([]byte("works")),
		},
	}

	go func() {
		buffer, err := d.lookup(h)
		if err != nil {
			t.Fatal("Lookup failed:", err)
		}

		if !bytes.Equal(buffer, []byte("helloworldworks")) {
			t.Fatal("Inserted and looked up buffer not equal")
		}
	}()

	time.Sleep(100 * time.Millisecond)

	d.addChunk(c1)
	d.addMetaData(md)
	d.addChunk(c2)
	d.addMetaData(md)
	d.addChunk(c3)
	d.addMetaData(md)
	d.addChunk(c3)
	d.addChunk(c3)

	buffer, err := d.lookup(h)
	if err != nil {
		t.Fatal("Lookup failed:", err)
	}

	if !bytes.Equal(buffer, []byte("helloworldworks")) {
		t.Fatal("Inserted and looked up buffer not equal")
	}
}
