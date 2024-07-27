package mini_lsm

import (
	"encoding/binary"
	"errors"
	"github.com/huandu/skiplist"
	"io"
	"os"
	"slices"
	"sync"
)

type Wal struct {
	file *os.File
	mu   sync.Mutex
}

func NewWal(p string) (*Wal, error) {
	//fmt.Printf("NewWal: %s\n", p)
	file, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	return &Wal{
		file: file,
	}, nil
}

func RecoverWal(p string, skiplist *skiplist.SkipList) (*Wal, error) {
	// fmt.Printf("RecoverWal: %s\n", p)
	file, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	contentLen := len(content)
	idx := 0
	for {
		if idx >= contentLen {
			break
		}
		keyLen := binary.BigEndian.Uint16(content[:2])
		content = content[2:]
		idx += 2
		key := content[:keyLen]
		idx += int(keyLen)
		content = content[keyLen:]
		valLen := binary.BigEndian.Uint16(content[:2])
		content = content[2:]
		idx += 2
		val := content[:valLen]
		idx += int(valLen)
		content = content[valLen:]
		skiplist.Set(Key(slices.Clone(key)), slices.Clone(val))
	}
	return &Wal{
		file: file,
	}, nil
}

func (w *Wal) Put(k KeyType, v []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := make([]byte, 2+2+len(k.Val)+len(v))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(k.Val)))
	copy(buf[2:], k.Val)
	binary.BigEndian.PutUint16(buf[2+len(k.Val):], uint16(len(v)))
	copy(buf[2+len(k.Val)+2:], v)
	for {
		n, err := w.file.Write(buf)
		if errors.Is(err, io.ErrShortWrite) {
			buf = buf[n:]
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	return nil
}

func (w *Wal) Close() error {
	//fmt.Printf("Close wal %s\n", w.file.Name())
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}
