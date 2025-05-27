package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type recordRef struct {
	file   string
	offset int64
}

type hashIndex map[string]recordRef

type Db struct {
	out            *os.File
	outOffset      int64
	index          hashIndex
	segments       []string
	segmentMaxSize int64
	dir            string
}

func Open(dir string, maxSize int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		out:            f,
		dir:            dir,
		index:          make(hashIndex),
		segmentMaxSize: maxSize,
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

func (db *Db) recover() error {
	files, err := filepath.Glob(filepath.Join(db.dir, "segment-*"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	files = append(files, filepath.Join(db.dir, outFileName))

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		var offset int64
		in := bufio.NewReader(f)
		for {
			var record entry
			n, err := record.DecodeFromReader(in)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			db.index[record.key] = recordRef{file: file, offset: offset}
			offset += int64(n)
		}
		if filepath.Base(file) != outFileName {
			db.segments = append(db.segments, file)
		} else {
			db.outOffset = offset
		}
	}
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	ref, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}
	file, err := os.Open(ref.file)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(ref.offset, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{key: key, value: value}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = recordRef{file: db.out.Name(), offset: db.outOffset}
		db.outOffset += int64(n)
		if db.outOffset >= db.segmentMaxSize {
			return db.rotateSegment()
		}
	}
	return err
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) rotateSegment() error {
	if err := db.out.Close(); err != nil {
		return err
	}
	segmentName := fmt.Sprintf("segment-%d", len(db.segments)+1)
	newPath := filepath.Join(db.dir, segmentName)
	if err := os.Rename(filepath.Join(db.dir, outFileName), newPath); err != nil {
		return err
	}
	db.segments = append(db.segments, newPath)
	f, err := os.OpenFile(filepath.Join(db.dir, outFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	db.out = f
	db.outOffset = 0
	return nil
}

func (db *Db) Compact() error {
	// Створюємо тимчасовий новий файл для злиття
	tmpPath := filepath.Join(db.dir, "segment-compacting")
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("compact: cannot create tmp file: %w", err)
	}
	defer tmpFile.Close()

	newIndex := make(hashIndex)
	var offset int64

	for key := range db.index {
		val, err := db.Get(key)
		if err != nil {
			continue
		}
		e := entry{key: key, value: val}
		data := e.Encode()

		if _, err := tmpFile.Write(data); err != nil {
			return fmt.Errorf("compact: write failed: %w", err)
		}

		newIndex[key] = recordRef{
			file:   tmpPath,
			offset: offset,
		}
		offset += int64(len(data))
	}

	// Закриваємо поточний файл і старі сегменти
	if err := db.out.Close(); err != nil {
		return fmt.Errorf("compact: close current-data: %w", err)
	}

	for _, seg := range db.segments {
		_ = os.Remove(seg) // ігноруємо помилки видалення
	}
	_ = os.Remove(filepath.Join(db.dir, outFileName))

	// Перейменовуємо тимчасовий файл у новий сегмент
	newSegName := fmt.Sprintf("segment-%d", len(db.segments)+1)
	newSegPath := filepath.Join(db.dir, newSegName)
	if err := os.Rename(tmpPath, newSegPath); err != nil {
		return fmt.Errorf("compact: rename failed: %w", err)
	}

	// 🛠 Оновлюємо file-шляхи у новому індексі
	for key, ref := range newIndex {
		ref.file = newSegPath
		newIndex[key] = ref
	}

	// Відкриваємо новий out файл
	out, err := os.OpenFile(filepath.Join(db.dir, outFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("compact: reopen current-data: %w", err)
	}

	// Оновлюємо стан бази
	db.out = out
	db.outOffset = 0
	db.index = newIndex
	db.segments = []string{newSegPath}

	return nil
}
