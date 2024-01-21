package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/chunk"
	"io"
	"net/http"
)

type Client struct {
	addr      string
	httpCli   http.Client
	offset    uint64
	currChunk chunk.Chunk
}

var errRetry = errors.New("retry the request")

// NewClient creates a new client
func NewClient(addr string) *Client {
	return &Client{addr: addr, httpCli: http.Client{}}
}

// Send sends messages to the server
func (c *Client) Send(messages []byte) error {
	resp, err := c.httpCli.Post(fmt.Sprintf("%s/write", c.addr), "application/octet-stream", bytes.NewReader(messages))
	if err != nil {
		return err
	}

	defer func(Body io.ReadCloser) {
		_ = Body.Close()

	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return fmt.Errorf("status code:: %d - error::%s ", resp.StatusCode, b.String())
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) Receive(temp []byte) ([]byte, error) {
	if temp == nil {
		temp = make([]byte, 1024*1024)
	}
	for {
		res, err := c.receive(temp)
		if !errors.Is(err, errRetry) {
			return res, err
		}
	}
}

// Receive receives messages from the server
func (c *Client) receive(temp []byte) ([]byte, error) {
	if temp == nil {
		temp = make([]byte, 1024*1024)
	}

	if err := c.updateCurrChunk(); err != nil {
		return nil, fmt.Errorf("error while updating current chunk %v, err %w", c.currChunk.Name, err)
	}

	resp, err := c.httpCli.Get(fmt.Sprintf("%s/read?offset=%d&maxSize=%d&chunk=%s", c.addr, c.offset, len(temp), c.currChunk.Name))
	if err != nil {
		return nil, fmt.Errorf("error while reading %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return nil, fmt.Errorf("reading: status code:: %d - error::%s ", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(temp[0:0])
	_, err = io.Copy(b, resp.Body)

	if err != nil {
		return nil, fmt.Errorf("error while copying resp %v", err)
	}

	if b.Len() == 0 {
		if !c.currChunk.Complete {
			if err := c.updateCurrChunkCompleteStatus(); err != nil {
				return nil, fmt.Errorf("error while updating current chunk complete status %v", err)
			}
			if !c.currChunk.Complete {
				if c.offset >= c.currChunk.Size {
					return nil, io.EOF
				}
				return nil, errRetry
			}
		}
		if c.offset < c.currChunk.Size {
			return nil, errRetry
		}
		if err := c.Ack(c.addr); err != nil {
			return nil, fmt.Errorf("error while acking %v", err)
		}
		c.currChunk = chunk.Chunk{}
		c.offset = 0
		return nil, errRetry
	}
	c.offset += uint64(b.Len())
	return b.Bytes(), nil

}

// Ack acks the current chunk
func (c *Client) Ack(addr string) error {
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/ack?chunk=%s&size=%d", addr, c.currChunk.Name, c.offset))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return fmt.Errorf("status code:: %d - error::%s ", resp.StatusCode, b.String())
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) updateCurrChunk() error {
	if c.currChunk.Name != "" {
		return nil
	}
	chunks, err := c.ListChunks()
	if err != nil {
		return fmt.Errorf("error while listing chunks %v", err)
	}
	if len(chunks) == 0 {
		return io.EOF
	}
	c.currChunk = chunks[0]
	return nil
}

// ListChunks lists all the chunks
func (c *Client) ListChunks() ([]chunk.Chunk, error) {
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/listChunks", c.addr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return nil, fmt.Errorf("status code:: %d - error::%s ", resp.StatusCode, b.String())
	}

	var chunks []chunk.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&chunks); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (c *Client) updateCurrChunkCompleteStatus() error {
	chunks, err := c.ListChunks()
	if err != nil {
		return fmt.Errorf("error while listing chunks %v", err)
	}
	for _, ch := range chunks {
		if ch.Name == c.currChunk.Name {
			c.currChunk = ch
			return nil
		}
	}
	return nil
}
