package dalog

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const (
	RecordLength = 8
)

type Store struct {
	*os.File
	mu   sync.RWMutex
	size uint64
	buf  *bufio.Writer
}

func NewStore(f *os.File) (*Store, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())

	return &Store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *Store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = uint64(s.size)
	dataLen := uint64(len(data))
	totalLen := dataLen + RecordLength

	// Create a buffer to hold the length prefix and the data
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint64(buf[:RecordLength], dataLen)
	copy(buf[RecordLength:], data)

	// Write the buffer to the store
	if _, err := s.buf.Write(buf); err != nil {
		return 0, 0, err
	}

	s.size += totalLen
	return totalLen, pos, nil
}

func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the size of the data
	sizeBuf := make([]byte, RecordLength)
	if _, err := s.File.ReadAt(sizeBuf, int64(pos)); err != nil {
		return nil, err
	}
	dataSize := binary.BigEndian.Uint64(sizeBuf)

	// Read the data
	readData := make([]byte, dataSize)
	if _, err := s.File.ReadAt(readData, int64(pos+RecordLength)); err != nil {
		return nil, err
	}
	return readData, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
