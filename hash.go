package p2p

import (
	"crypto/sha512"
	"encoding/hex"
)

func hash(buffer []byte) string {
	h := sha512.Sum512(buffer)
	return hex.EncodeToString(h[:])
}

func splitAndHash(buffer []byte, count int) (splitHashes []string, splitBuffers [][]byte) {
	if count < 1 || count > len(buffer) {
		panic("Count out of range")
	}

	s := len(buffer) / count
	splitBuffers = make([][]byte, count)
	splitHashes = make([]string, count)

	for i := 0; i < count; i++ {
		b := buffer[:s]
		splitBuffers[i] = b
		splitHashes[i] = hash(b)
		buffer = buffer[s:]
	}

	return
}
