package replication

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type Storage struct {
	client          *clientv3.Client
	currentInstance string
}

func NewStorage(client *clientv3.Client, currentInstance string) *Storage {
	return &Storage{client: client, currentInstance: currentInstance}
}

func (s *Storage) Init(ctx context.Context, category, fileName string) error {
	resp, err := s.client.Get(ctx, "peers/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("error getting peers from etcd %w", err)
	}

	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), "peers/")
		if key == s.currentInstance {
			continue
		}
		_, err := s.client.Put(ctx, fmt.Sprintf("replication/%s/%s/%s", key, category, fileName), s.currentInstance)
		if err != nil {
			return fmt.Errorf("could not write to replication queue %q %q: %w", key, string(kv.Value), err)
		}
	}
	return nil
}
