package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	addr    string
	httpCli http.Client
	offset  uint64
}

func NewClient(addr string) *Client {
	return &Client{addr: addr, httpCli: http.Client{}}
}

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

	resp, err := c.httpCli.Get(fmt.Sprintf("%s/read?offset=%d&maxSize=%d", c.addr, c.offset, len(temp)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = io.Copy(&b, resp.Body)
		return nil, fmt.Errorf("status code:: %d - error::%s ", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(temp[0:0])
	_, err = io.Copy(b, resp.Body)

	if err != nil {
		return nil, err
	}

	if b.Len() == 0 {
		if err := c.Ack(c.addr); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	c.offset += uint64(b.Len())
	return b.Bytes(), nil

}

func (c *Client) Ack(addr string) error {
	resp, err := c.httpCli.Get(fmt.Sprintf("%s/ack", addr))
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
