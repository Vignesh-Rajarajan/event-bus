package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/chunk"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
func (c *Client) Send(category string, messages []byte) error {
	u := url.Values{}
	u.Add("category", category)
	resp, err := c.httpCli.Post(fmt.Sprintf("%s/write?%s", c.addr, u.Encode()), "application/octet-stream", bytes.NewReader(messages))
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

// Process receives messages from the server
func (c *Client) Process(category string, temp []byte, processFn func([]byte) error) error {
	if temp == nil {
		temp = make([]byte, 1024*1024)
	}
	for {
		err := c.process(category, temp, processFn)
		if !errors.Is(err, errRetry) {
			return err
		}
	}
}

func (c *Client) process(category string, temp []byte, processFn func([]byte) error) error {

	if err := c.updateCurrChunk(category); err != nil {
		return fmt.Errorf("error while updating current chunk %v, err %w", c.currChunk.Name, err)
	}

	u := url.Values{}
	u.Add("category", category)
	u.Add("offset", strconv.Itoa(int(c.offset)))
	u.Add("chunk", c.currChunk.Name)
	u.Add("maxSize", strconv.Itoa(len(temp)))
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/read?%s", c.addr, u.Encode()))
	if err != nil {
		return fmt.Errorf("error while reading %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return fmt.Errorf("process: status code:: %d - error::%s ", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(temp[0:0])
	_, err = io.Copy(b, resp.Body)

	if err != nil {
		return fmt.Errorf("error while copying resp %v", err)
	}

	if b.Len() == 0 {
		if !c.currChunk.Complete {
			if err := c.updateCurrChunkCompleteStatus(category); err != nil {
				return fmt.Errorf("error while updating current chunk complete status %v", err)
			}
			if !c.currChunk.Complete {
				if c.offset >= c.currChunk.Size {
					return io.EOF
				}
				return errRetry
			}
		}
		if c.offset < c.currChunk.Size {
			return errRetry
		}
		if err := c.Ack(category, c.addr); err != nil {
			return fmt.Errorf("error while acking %v", err)
		}
		c.currChunk = chunk.Chunk{}
		c.offset = 0
		return errRetry
	}
	err = processFn(b.Bytes())
	if err == nil {
		c.offset += uint64(b.Len())
	}
	return err
}

// Ack acks the current chunk
func (c *Client) Ack(category string, addr string) error {
	u := url.Values{}
	u.Add("category", category)
	u.Add("chunk", c.currChunk.Name)
	u.Add("size", strconv.Itoa(int(c.offset)))
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/ack?%s", addr, u.Encode()))
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

func (c *Client) updateCurrChunk(category string) error {
	if c.currChunk.Name != "" {
		return nil
	}
	chunks, err := c.ListChunks(category)
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
func (c *Client) ListChunks(category string) ([]chunk.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/listChunks?%s", c.addr, u.Encode()))
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

func (c *Client) updateCurrChunkCompleteStatus(category string) error {
	chunks, err := c.ListChunks(category)
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
