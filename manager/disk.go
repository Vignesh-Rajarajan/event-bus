package manager

import (
	"io"
	"log"
	"os"
)

type EventBusOnDisk struct {
	file *os.File
}

//var _ EventManager = (*EventBusOnDisk)(nil)

func NewEventBusOnDisk(filename string) (*EventBusOnDisk, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &EventBusOnDisk{file: file}, nil
}

func (c *EventBusOnDisk) Write(msg []byte) error {
	_, err := c.file.Write(msg)
	return err
}

func (c *EventBusOnDisk) Read(offset, maxSize uint64, w io.Writer) error {
	buff := make([]byte, maxSize)
	n, err := c.file.ReadAt(buff, int64(offset))

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

func (c *EventBusOnDisk) Ack() error {
	log.Default().Println("Ack called, acking all events")
	if err := c.file.Truncate(0); err != nil {
		return err
	}
	if _, err := c.file.Seek(0, 0); err != nil {
		return err
	}
	return nil
}
