package p2p

import "testing"

func TestHash(t *testing.T) {
	b := []byte("HelloHelloHelloHelloHelloHello")
	h := hash(b)
	hs, bs := splitAndHash(b, 6)

	b = b[:0]
	for i := 0; i < 6; i++ {
		if hash(bs[i]) != hs[i] {
			t.Fatal("Wrong hashes")
		}
		b = append(b, bs[i]...)
	}

	if hash(b) != h {
		t.Fatal("Wrong hash")
	}
}
