package mini_lsm

import (
	"encoding/binary"
	"errors"
	"github.com/Zhanghailin1995/mini-lsm/utils"
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
		lIdx := 0
		keyLen := binary.BigEndian.Uint16(content[lIdx:2])
		idx += 2
		lIdx += 2
		key := content[lIdx : lIdx+int(keyLen)]
		idx += int(keyLen)
		lIdx += int(keyLen)
		valLen := binary.BigEndian.Uint16(content[lIdx : lIdx+2])
		idx += 2
		lIdx += 2
		val := content[lIdx : lIdx+int(valLen)]
		lIdx += int(valLen)
		idx += int(valLen)
		checksum := binary.BigEndian.Uint32(content[lIdx : lIdx+4])
		if checksum != utils.Crc32(content[:lIdx]) {
			return nil, errors.New("checksum error")
		}
		lIdx += 4
		idx += 4
		content = content[lIdx:]
		skiplist.Set(Key(slices.Clone(key)), slices.Clone(val))
	}
	return &Wal{
		file: file,
	}, nil
}

func (w *Wal) Put(k KeyType, v []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := make([]byte, 2+2+len(k.Val)+len(v)+4)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(k.Val)))
	copy(buf[2:], k.Val)
	binary.BigEndian.PutUint16(buf[2+len(k.Val):], uint16(len(v)))
	copy(buf[2+len(k.Val)+2:], v)
	checksum := utils.Crc32(buf[:len(buf)-4])
	binary.BigEndian.PutUint32(buf[len(buf)-4:], checksum)
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
