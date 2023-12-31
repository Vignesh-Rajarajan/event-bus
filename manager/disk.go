package manager

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/chunk"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const maxOnDiskChunkSize = 20 * 1024 * 1024

type EventBusOnDisk struct {
	dirname       string
	mu            sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	filePointers  map[string]*os.File
}

var _ EventManager = (*EventBusOnDisk)(nil)

func NewEventBusOnDisk(dirname string) *EventBusOnDisk {
	return &EventBusOnDisk{
		dirname:      dirname,
		filePointers: make(map[string]*os.File),
	}
}

func (c *EventBusOnDisk) Write(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastChunk == "" || (c.lastChunkSize+uint64(len(msg)) > maxOnDiskChunkSize) {
		c.lastChunk = fmt.Sprintf("chunk%d", c.lastChunkIdx)
		c.lastChunkIdx++
		c.lastChunkSize = 0
	}

	fp, err := c.getFilePointer(c.lastChunk)
	if err != nil {
		return fmt.Errorf("error while getting file pointer %v for chunk %s while writing", err, c.lastChunk)
	}
	_, err = fp.Write(msg)
	if err != nil {
		return fmt.Errorf("error while writing to file %v for chunk %s", err, c.lastChunk)
	}
	c.lastChunkSize += uint64(len(msg))
	return nil
}

func (c *EventBusOnDisk) Read(chunk string, offset, maxSize uint64, w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	chunk = filepath.Clean(chunk)

	_, err := os.Stat(filepath.Join(c.dirname, chunk))
	if err != nil {
		return fmt.Errorf("chunk %s not found, err %v", chunk, err)
	}
	fp, err := c.getFilePointer(chunk)
	if err != nil {
		return fmt.Errorf("error while getting file pointer %v for chunk %s while reading", err, chunk)
	}
	buff := make([]byte, maxSize)
	n, err := fp.ReadAt(buff, int64(offset))

	if n == 0 {
		if err == io.EOF {
			return nil
		}
		return err
	}

	truncated, _, err := getTillLastDelimiter(buff[0:n])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil

}

func (c *EventBusOnDisk) Ack(chunk string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if chunk == c.lastChunk {
		return fmt.Errorf("cannot ack last chunk %s as it's incomplete", chunk)
	}
	chunk = filepath.Clean(chunk)
	chunkFile := filepath.Join(c.dirname, chunk)

	_, err := os.Stat(chunkFile)
	if err != nil {
		return fmt.Errorf("chunk %s not found, err %v", chunk, err)
	}
	if err := os.Remove(chunkFile); err != nil {
		return fmt.Errorf("error while removing chunk %s, err %v", chunk, err)
	}
	fp, ok := c.filePointers[chunk]
	if ok {
		if err := fp.Close(); err != nil {
			return fmt.Errorf("error while closing file pointer %v for chunk %s", err, chunk)
		}
	}
	delete(c.filePointers, chunk)
	return nil
}

func (c *EventBusOnDisk) ListChunks() ([]chunk.Chunk, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var chunks []chunk.Chunk
	files, err := os.ReadDir(c.dirname)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		chunks = append(chunks, chunk.Chunk{Name: file.Name(), Complete: file.Name() != c.lastChunk})
	}
	return chunks, nil
}

func (c *EventBusOnDisk) getFilePointer(chunk string) (*os.File, error) {
	fp, ok := c.filePointers[chunk]
	if ok {
		return fp, nil
	}
	fp, err := os.OpenFile(filepath.Join(c.dirname, chunk), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	c.filePointers[chunk] = fp
	return fp, nil

}
