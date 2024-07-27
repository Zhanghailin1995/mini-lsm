package mini_lsm

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type ManifestRecordType uint8

const (
	Flush ManifestRecordType = iota
	NewMemtable
	Compaction
)

type Manifest struct {
	file *os.File
	mu   sync.Mutex
}

type ManifestRecord interface {
	isManifestRecord()
	recordType() ManifestRecordType
}

type FlushRecord struct {
	Flush uint32
}

func (f *FlushRecord) isManifestRecord() {}

func (f *FlushRecord) recordType() ManifestRecordType {
	return Flush
}

type NewMemtableRecord struct {
	NewMemtable uint32
}

func (n *NewMemtableRecord) isManifestRecord() {}

func (n *NewMemtableRecord) recordType() ManifestRecordType {
	return NewMemtable
}

type CompactionRecord struct {
	CompactionTask *CompactionTask
	SstIds         []uint32
}

func (c *CompactionRecord) recordType() ManifestRecordType {
	return Compaction
}

func (c *CompactionRecord) isManifestRecord() {}

func NewManifest(p string) (*Manifest, error) {
	file, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		file: file,
	}, nil
}

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}

func RecoverManifest(p string) (*Manifest, []ManifestRecord, error) {
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, err
	}
	content, err := os.ReadFile(p)
	if err != nil {
		return nil, nil, err
	}
	var records []ManifestRecord
	idx := 0
	contentLen := len(content)
	for {
		if idx >= contentLen {
			break
		}
		l := binary.BigEndian.Uint32(content)
		content = content[4:]
		recordType := content[0]
		content = content[1:]
		recordBytes := content[:int(l)-1]
		content = content[int(l)-1:]
		var record ManifestRecord
		var unknownTask json.RawMessage
		switch ManifestRecordType(recordType) {
		case Flush:
			record = &FlushRecord{}
			err := json.Unmarshal(recordBytes, record)
			if err != nil {
				return nil, nil, err
			}
		case NewMemtable:
			record = &NewMemtableRecord{}
			err := json.Unmarshal(recordBytes, record)
			if err != nil {
				return nil, nil, err
			}
		case Compaction:
			record = &CompactionRecord{
				CompactionTask: &CompactionTask{
					Task: &unknownTask,
				},
			}
			err := json.Unmarshal(recordBytes, record)
			if err != nil {
				return nil, nil, err
			}
			t := record.(*CompactionRecord).CompactionTask.CompactionType
			switch t {
			case ForceFull:
				record.(*CompactionRecord).CompactionTask.Task = &ForceFullCompactionTask{}
			case Leveled:
				record.(*CompactionRecord).CompactionTask.Task = &LeveledCompactionTask{}
			case Tiered:
				record.(*CompactionRecord).CompactionTask.Task = &TieredCompactionTask{}
			case Simple:
				record.(*CompactionRecord).CompactionTask.Task = &SimpleLeveledCompactionTask{}
			}
			err = json.Unmarshal(unknownTask, record.(*CompactionRecord).CompactionTask.Task)
			if err != nil {
				return nil, nil, err
			}
		}

		records = append(records, record)
		idx += int(l) + 4
	}
	return &Manifest{
		file: f,
	}, records, nil
}

func (m *Manifest) AddRecord(record ManifestRecord) error {
	return m.AddRecordWhenInit(record)
}

func (m *Manifest) AddRecordWhenInit(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// write record to file
	// | len | record type | record |
	bytes, err2 := json.Marshal(record)
	if err2 != nil {
		return err2
	}
	l := 1 + len(bytes)
	buf := make([]byte, 4+l)
	binary.BigEndian.PutUint32(buf[:4], uint32(l))
	buf[4] = byte(record.recordType())
	copy(buf[5:], bytes)
	// write record type
	for {
		n, err := m.file.Write(buf)
		if errors.Is(err, io.ErrShortWrite) {
			buf = buf[n:]
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	err := m.file.Sync()
	if err != nil {
		return err
	}
	return nil
}
