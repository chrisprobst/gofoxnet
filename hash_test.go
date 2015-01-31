package gofoxnet

import "testing"

func TestHash(t *testing.T) {
	b := []byte("HelloHelloHelloHelloHelloHello")
	h := Hash(b)
	hs, bs := SplitAndHash(b, 6)

	b = b[:0]
	for i := 0; i < 6; i++ {
		if Hash(bs[i]) != hs[i] {
			t.Fatal("Wrong hashes")
		}
		b = append(b, bs[i]...)
	}

	if Hash(b) != h {
		t.Fatal("Wrong hash")
	}
}
