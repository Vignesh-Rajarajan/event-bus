package manager

import (
	"io"
	"log"
)

type EventBusInMemory struct {
	buff []byte
}

var _ EventManager = (*EventBusInMemory)(nil)

func (c *EventBusInMemory) Write(msg []byte) error {
	c.buff = append(c.buff, msg...)
	return nil
}

func (c *EventBusInMemory) Read(offset, maxSize uint64, w io.Writer) error {
	//log.Default().Println("Receive called with offset", offset, "maxSize", maxSize)
	maxOffset := uint64(len(c.buff))
	if offset >= maxOffset {
		return nil
	}

	if offset+maxSize >= maxOffset {
		w.Write(c.buff[offset:])
		return nil
	}

	truncated, _, err := getTillLastDelimiter(c.buff[offset : offset+maxSize])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

func (c *EventBusInMemory) Ack() error {
	log.Default().Println("Ack called, acking all events")
	c.buff = nil
	return nil
}
