package manager

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/chunk"
	"io"
	"sync"
)

const maxChunkSize = 10 * 1024 * 1024

type EventBusInMemory struct {
	mu            sync.RWMutex
	lastChunkName string
	lastChunkSize uint64
	lastChunkIdx  uint64
	buffs         map[string][]byte
}

var _ EventManager = (*EventBusInMemory)(nil)

func (c *EventBusInMemory) Write(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastChunkName == "" || (c.lastChunkSize+uint64(len(msg)) > maxChunkSize) {
		c.lastChunkName = fmt.Sprintf("chunk-%d", c.lastChunkIdx)
		c.lastChunkIdx++
		c.lastChunkSize = 0
	}
	if c.buffs == nil {
		c.buffs = make(map[string][]byte)
	}
	c.buffs[c.lastChunkName] = append(c.buffs[c.lastChunkName], msg...)
	c.lastChunkSize += uint64(len(msg))
	return nil
}

func (c *EventBusInMemory) Read(chunk string, offset, maxSize uint64, w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	buff, ok := c.buffs[chunk]
	if !ok {
		return fmt.Errorf("chunk %s not found", chunk)
	}
	maxOffset := uint64(len(buff))
	if offset >= maxOffset {
		return nil
	}

	if offset+maxSize >= maxOffset {
		w.Write(buff[offset:])
		return nil
	}

	truncated, _, err := getTillLastDelimiter(buff[offset : offset+maxSize])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

func (c *EventBusInMemory) Ack(chunk string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.buffs[chunk]
	if !ok {
		return fmt.Errorf("chunk %s not found", chunk)
	}
	if chunk == c.lastChunkName {
		return fmt.Errorf("chunk %s is currently not filled and written into and is not ackable", chunk)
	}
	delete(c.buffs, chunk)
	return nil
}

func (c *EventBusInMemory) ListChunks() ([]chunk.Chunk, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var chunks []chunk.Chunk
	for k := range c.buffs {
		chunks = append(chunks, chunk.Chunk{Name: k, Complete: c.lastChunkName != k})
	}
	return chunks, nil
}
