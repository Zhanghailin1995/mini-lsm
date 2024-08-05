package mini_lsm

import (
	"bufio"
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
	w    *bufio.Writer
	file *os.File
	mu   sync.Mutex
	cnt  int
	size int
}

func NewWal(p string) (*Wal, error) {
	//fmt.Printf("NewWal: %s\n", p)
	file, err := os.Create(p)
	bufWriter := bufio.NewWriter(file)
	if err != nil {
		return nil, err
	}
	return &Wal{
		w:    bufWriter,
		file: file,
		cnt:  0,
		size: 0,
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
		// read key len
		keyLen := binary.BigEndian.Uint16(content[lIdx:2])
		idx += 2
		lIdx += 2
		// read key
		key := content[lIdx : lIdx+int(keyLen)]
		idx += int(keyLen)
		lIdx += int(keyLen)
		// read ts
		ts := binary.BigEndian.Uint64(content[lIdx : lIdx+8])
		idx += 8
		lIdx += 8

		// read val len
		valLen := binary.BigEndian.Uint16(content[lIdx : lIdx+2])
		idx += 2
		lIdx += 2

		// read val
		val := content[lIdx : lIdx+int(valLen)]
		lIdx += int(valLen)
		idx += int(valLen)

		// check checksum
		checksum := binary.BigEndian.Uint32(content[lIdx : lIdx+4])
		if checksum != utils.Crc32(content[:lIdx]) {
			return nil, errors.New("checksum error")
		}
		lIdx += 4
		idx += 4
		// advance idx
		content = content[lIdx:]
		skiplist.Set(KeyFromBytesWithTs(key, ts), slices.Clone(val))
	}
	w := bufio.NewWriter(file)
	return &Wal{
		w:    w,
		file: file,
	}, nil
}

func (w *Wal) Put(k KeyBytes, v []byte) error {
	//fmt.Printf("Put wal %s\n", w.file.Name())
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := make([]byte, 2+k.RawLen()+2+len(v)+4)
	// put key len
	binary.BigEndian.PutUint16(buf[:2], uint16(len(k.Val)))
	// put key
	copy(buf[2:], k.Val)
	// put ts
	binary.BigEndian.PutUint64(buf[2+len(k.Val):], k.Ts)
	// put val len
	binary.BigEndian.PutUint16(buf[2+k.RawLen():], uint16(len(v)))
	// put val
	copy(buf[2+k.RawLen()+2:], v)
	checksum := utils.Crc32(buf[:len(buf)-4])
	binary.BigEndian.PutUint32(buf[len(buf)-4:], checksum)
	//if strings.Contains(w.file.Name(), "00038.wal") {
	//	atomic.AddUint64(&x, uint64(len(buf)))
	//	atomic.AddInt32(&y, 1)
	//	println(string(k.KeyRef()))
	//}
	w.cnt++
	w.size += len(buf)
	for {
		n, err := w.w.Write(buf)
		if errors.Is(err, io.ErrShortWrite) {
			buf = buf[n:]
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	// do we need flush?
	err := w.w.Flush()
	if err != nil && !errors.Is(err, io.ErrShortWrite) {
		return err
	}
	return nil
}

func (w *Wal) Close() error {
	//fmt.Printf("Close wal %s\n", w.file.Name())
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		err := w.w.Flush()
		if err != nil && errors.Is(err, io.ErrShortWrite) {
			continue
		} else if err == nil {
			break
		} else {
			return err
		}
	}
	return w.file.Close()
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		err := w.w.Flush()
		if err != nil && errors.Is(err, io.ErrShortWrite) {
			continue
		} else if err == nil {
			break
		} else {
			return err
		}
	}

	return w.file.Sync()
}
