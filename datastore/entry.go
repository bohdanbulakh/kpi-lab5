package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

type entry struct {
	key, value string
	hash       string
}

func (e *entry) Encode() []byte {
	kl, vl := len(e.key), len(e.value)
	hash := sha1.Sum([]byte(e.value))
	e.hash = hex.EncodeToString(hash[:])
	hl := len(e.hash)

	size := 4 + 4 + kl + 4 + vl + 4 + hl
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res[0:], uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)

	binary.LittleEndian.PutUint32(res[8+kl:], uint32(vl))
	copy(res[12+kl:], e.value)

	binary.LittleEndian.PutUint32(res[12+kl+vl:], uint32(hl))
	copy(res[16+kl+vl:], []byte(e.hash))

	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyStart := 8
	keyEnd := int(kl) + keyStart

	vl := binary.LittleEndian.Uint32(input[keyEnd:])
	valStart := keyEnd + 4
	valEnd := valStart + int(vl)

	hl := binary.LittleEndian.Uint32(input[valEnd:])
	hashStart := valEnd + 4
	hashEnd := hashStart + int(hl)

	e.key = string(input[keyStart:keyEnd])
	e.value = string(input[valStart:valEnd])
	e.hash = string(input[hashStart:hashEnd])
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}

	totalSize := int(binary.LittleEndian.Uint32(sizeBuf))
	buf := make([]byte, totalSize)
	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read record: %w", err)
	}

	e.Decode(buf)
	return n, nil
}
