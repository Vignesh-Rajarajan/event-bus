package replication

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

const defaultTimeout = 10 * time.Second

type Client struct {
	cli    *clientv3.Client
	prefix string
}

type Result struct {
	Key   string
	Value string
}

type Peer struct {
	Addr string
	Name string
}

type Chunk struct {
	OwnedBy  string
	Category string
	FileName string
}

type Option clientv3.OpOption

func NewClient(addr []string, clusterName string) (*Client, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: defaultTimeout,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return nil, fmt.Errorf("error putting test key in etcd %w", err)
	}
	return &Client{cli: etcdClient, prefix: fmt.Sprintf("events/%s/", clusterName)}, nil
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.cli.Put(ctx, c.prefix+key, value)
	return err
}

func (c *Client) Get(ctx context.Context, key string, opts ...Option) ([]Result, error) {
	etcdOps := make([]clientv3.OpOption, len(opts))
	for _, opt := range opts {
		etcdOps = append(etcdOps, clientv3.OpOption(opt))
	}
	etcdResp, err := c.cli.Get(ctx, c.prefix+key, etcdOps...)
	if err != nil {
		return nil, err
	}

	var results []Result
	for _, kv := range etcdResp.Kvs {
		results = append(results, Result{Key: string(kv.Key), Value: string(kv.Value)})
	}
	return results, nil
}

func (c *Client) ListPeers(ctx context.Context) ([]Peer, error) {
	resp, err := c.cli.Get(ctx, "peers/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("error getting peers from etcd %w", err)
	}
	var peers []Peer
	for _, kv := range resp.Kvs {
		peers = append(peers, Peer{Addr: string(kv.Value), Name: strings.TrimPrefix(string(kv.Key), c.prefix+"peers/")})
	}
	return peers, nil
}

func (c *Client) RegisterPeer(ctx context.Context, peer Peer) error {
	_, err := c.cli.Put(ctx, "peers/"+peer.Name, peer.Addr)
	return err
}

func (c *Client) AddChunkToReplicationQueue(ctx context.Context, targetInstance string, chunk Chunk) error {
	_, err := c.cli.Put(ctx, fmt.Sprintf("replication/%s/%s/%s", targetInstance, chunk.Category, chunk.FileName), chunk.OwnedBy)
	return err
}
