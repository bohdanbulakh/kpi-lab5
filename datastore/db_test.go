package datastore

import (
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, 1000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned: expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth and rotation", func(t *testing.T) {
		beforeSegments, _ := db.Size()
		for _, pair := range pairs {
			_ = db.Put(pair[0], pair[1])
		}
		afterSegments, _ := db.Size()
		if afterSegments <= beforeSegments {
			t.Errorf("Expected at least one new segment file after exceeding size limit")
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp, 100)
		if err != nil {
			t.Fatal(err)
		}
		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}
		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})

	t.Run("compact segments", func(t *testing.T) {
		db.segmentMaxSize = 35

		_ = db.Put("rotate1", "long-value-to-trigger-rotation")
		_ = db.Put("rotate2", "another-long-value")

		before := len(db.segments)

		_ = db.Put("u1", "old")
		_ = db.Put("u1", "new")

		err := db.Compact()
		if err != nil {
			t.Fatalf("Compact() failed: %v", err)
		}

		val, err := db.Get("u1")
		if err != nil || val != "new" {
			t.Errorf("Expected u1 = 'new', got %s (err: %v)", val, err)
		}

		after := len(db.segments)
		if after >= before {
			t.Errorf("Expected fewer segments after compaction (before: %d, after: %d)", before, after)
		}
	})

}
