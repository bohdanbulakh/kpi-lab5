package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type recordRef struct {
	file   string
	offset int64
}

type hashIndex map[string]recordRef

type writeRequest struct {
	key   string
	value string
	resp  chan error
}

type Db struct {
	out            *os.File
	outOffset      int64
	index          hashIndex
	indexLock      sync.RWMutex
	segments       []string
	segmentMaxSize int64
	dir            string

	writeChan chan writeRequest
	quitChan  chan struct{}
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
		writeChan:      make(chan writeRequest),
		quitChan:       make(chan struct{}),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	go db.writerLoop()

	return db, nil
}

func (db *Db) writerLoop() {
	for {
		select {
		case req := <-db.writeChan:
			err := db.performPut(req.key, req.value)
			req.resp <- err
		case <-db.quitChan:
			return
		}
	}
}

func (db *Db) performPut(key, value string) error {
	e := entry{key: key, value: value}
	data := e.Encode()
	dataLen := int64(len(data)) // Додано визначення довжини даних

	db.indexLock.Lock()
	defer db.indexLock.Unlock()

	// Виправлено умову: перевіряємо, чи додавання нового запису перевищить ліміт
	if db.outOffset+dataLen > db.segmentMaxSize {
		if err := db.rotateSegment(); err != nil {
			return fmt.Errorf("rotation failed: %w", err)
		}
	}

	n, err := db.out.Write(data)
	if err != nil {
		return err
	}

	db.index[key] = recordRef{
		file:   db.out.Name(),
		offset: db.outOffset,
	}
	db.outOffset += int64(n)

	return nil
}

func (db *Db) Put(key, value string) error {
	resp := make(chan error)
	db.writeChan <- writeRequest{key: key, value: value, resp: resp}
	err := <-resp

	db.indexLock.RLock()
	ref, ok := db.index[key]
	db.indexLock.RUnlock()
	fmt.Printf("PUT DONE: key=%s, ok=%v, file=%s, offset=%d\n", key, ok, ref.file, ref.offset)

	return err
}

func (db *Db) Get(key string) (string, error) {
	db.indexLock.RLock()
	ref, ok := db.index[key]
	db.indexLock.RUnlock()

	if !ok {
		fmt.Printf("GET: key=%s NOT FOUND in index\n", key)
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

	hash := sha1.Sum([]byte(record.value))
	if record.hash != hex.EncodeToString(hash[:]) {
		return "", fmt.Errorf("data integrity error: hash mismatch")
	}

	return record.value, nil
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

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) Close() error {
	close(db.quitChan)
	return db.out.Close()
}

func (db *Db) Compact() error {
	tmpPath := filepath.Join(db.dir, "segment-compacting")
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("compact: cannot create tmp file: %w", err)
	}
	defer tmpFile.Close()

	newIndex := make(hashIndex)
	var offset int64

	db.indexLock.Lock()
	defer db.indexLock.Unlock()

	for key, ref := range db.index {
		file, err := os.Open(ref.file)
		if err != nil {
			continue
		}

		if _, err := file.Seek(ref.offset, io.SeekStart); err != nil {
			file.Close()
			continue
		}

		var rec entry
		if _, err := rec.DecodeFromReader(bufio.NewReader(file)); err != nil {
			file.Close()
			continue
		}
		file.Close()

		data := rec.Encode()
		if _, err := tmpFile.Write(data); err != nil {
			return fmt.Errorf("compact: write failed: %w", err)
		}

		newIndex[key] = recordRef{
			file:   tmpPath, // Тимчасовий шлях, буде змінено після перейменування
			offset: offset,
		}
		offset += int64(len(data))
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("compact: failed to close tmp file: %w", err)
	}

	if err := db.out.Close(); err != nil {
		return fmt.Errorf("compact: close current-data: %w", err)
	}

	// Видаляємо всі старі сегменти та current-data
	for _, seg := range db.segments {
		_ = os.Remove(seg)
	}
	_ = os.Remove(filepath.Join(db.dir, outFileName))

	// Перейменовуємо тимчасовий файл у новий сегмент
	newSegName := fmt.Sprintf("segment-%d", len(db.segments)+1)
	newSegPath := filepath.Join(db.dir, newSegName)
	if err := os.Rename(tmpPath, newSegPath); err != nil {
		return fmt.Errorf("compact: rename failed: %w", err)
	}

	// Оновлюємо індекс з новими шляхами
	for key, ref := range newIndex {
		ref.file = newSegPath
		newIndex[key] = ref
	}

	// Відкриваємо новий current-data
	out, err := os.OpenFile(filepath.Join(db.dir, outFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("compact: reopen current-data: %w", err)
	}

	// Оновлюємо стан бази даних
	db.out = out
	db.outOffset = 0
	db.index = newIndex
	db.segments = []string{newSegPath} // Зберігаємо лише новий компактний сегмент

	return nil
}
