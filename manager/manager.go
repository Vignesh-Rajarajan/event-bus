package manager

import (
	"bytes"
	"context"
	"errors"
	"github.com/Vignesh-Rajarajan/event-bus/chunk"
	"io"
)

type EventManager interface {
	Read(chunk string, offset, maxSize uint64, w io.Writer) error
	Write(ctx context.Context, body []byte) error
	Ack(chunk string, size uint64) error
	ListChunks() ([]chunk.Chunk, error)
}

func getTillLastDelimiter(temp []byte) (truncated []byte, rest []byte, err error) {
	n := len(temp)
	if n == 0 {
		return temp, nil, nil
	}

	if temp[n-1] == '\n' {
		return temp, nil, nil
	}

	lastIdx := bytes.LastIndexByte(temp, '\n')
	if lastIdx < 0 {
		return nil, nil, errors.New("buffer too small")
	}
	return temp[:lastIdx+1], temp[lastIdx+1:], nil
}
