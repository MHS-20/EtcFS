package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Entry struct {
	Ino        uint64
	LogicalOff uint64
	DiskOff    uint64
	Length     uint64
	Generation uint64
	Committed  bool
}

type WAL struct {
	mu sync.Mutex
	f  *os.File
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	return &WAL{f: f}, nil
}

func (w *WAL) Append(e *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeEntry(e)
}

func (w *WAL) MarkCommitted(ino, logicalOff uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeEntry(&Entry{Ino: ino, LogicalOff: logicalOff, Committed: true})
}

func (w *WAL) writeEntry(e *Entry) error {
	flags := uint8(0)
	if e.Committed {
		flags = 1
	}
	buf := make([]byte, 49)
	buf[0] = flags
	binary.LittleEndian.PutUint64(buf[1:9], e.Ino)
	binary.LittleEndian.PutUint64(buf[9:17], e.LogicalOff)
	binary.LittleEndian.PutUint64(buf[17:25], e.DiskOff)
	binary.LittleEndian.PutUint64(buf[25:33], e.Length)
	binary.LittleEndian.PutUint64(buf[33:41], e.Generation)
	_, err := w.f.Write(buf)
	if err == nil {
		err = w.f.Sync()
	}
	return err
}

func (w *WAL) Replay(cb func(e *Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	buf := make([]byte, 49)
	uncommitted := make(map[string]*Entry)
	for {
		n, _ := w.f.Read(buf)
		if n < 49 {
			break
		}
		e := &Entry{
			Committed:  buf[0]&1 != 0,
			Ino:        binary.LittleEndian.Uint64(buf[1:9]),
			LogicalOff: binary.LittleEndian.Uint64(buf[9:17]),
			DiskOff:    binary.LittleEndian.Uint64(buf[17:25]),
			Length:     binary.LittleEndian.Uint64(buf[25:33]),
			Generation: binary.LittleEndian.Uint64(buf[33:41]),
		}
		key := fmt.Sprintf("%d-%d", e.Ino, e.LogicalOff)
		if e.Committed {
			delete(uncommitted, key)
		} else {
			uncommitted[key] = e
		}
	}
	for _, e := range uncommitted {
		if err := cb(e); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Truncate(0)
}

func (w *WAL) Close() error {
	return w.f.Close()
}
