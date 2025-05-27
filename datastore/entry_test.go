package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"testing"
)

func TestEntry_EncodeDecode(t *testing.T) {
	original := entry{key: "key", value: "value"}
	encoded := original.Encode()

	var decoded entry
	decoded.Decode(encoded)

	if decoded.key != original.key {
		t.Errorf("expected key %q, got %q", original.key, decoded.key)
	}
	if decoded.value != original.value {
		t.Errorf("expected value %q, got %q", original.value, decoded.value)
	}

	expectedHash := sha1.Sum([]byte(original.value))
	expectedHashHex := hex.EncodeToString(expectedHash[:])
	if decoded.hash != expectedHashHex {
		t.Errorf("expected hash %q, got %q", expectedHashHex, decoded.hash)
	}
}

func TestEntry_DecodeFromReader(t *testing.T) {
	original := entry{key: "my-key", value: "super-secure-value"}
	encoded := original.Encode()

	var decoded entry
	n, err := decoded.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeFromReader error: %v", err)
	}
	if n != len(encoded) {
		t.Errorf("expected %d bytes read, got %d", len(encoded), n)
	}

	if decoded.key != original.key {
		t.Errorf("expected key %q, got %q", original.key, decoded.key)
	}
	if decoded.value != original.value {
		t.Errorf("expected value %q, got %q", original.value, decoded.value)
	}

	expectedHash := sha1.Sum([]byte(original.value))
	expectedHashHex := hex.EncodeToString(expectedHash[:])
	if decoded.hash != expectedHashHex {
		t.Errorf("expected hash %q, got %q", expectedHashHex, decoded.hash)
	}
}
