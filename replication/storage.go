package replication

import (
	"context"
	"fmt"
)

type Storage struct {
	client          *Client
	currentInstance string
}

func NewStorage(client *Client, currentInstance string) *Storage {
	return &Storage{client: client, currentInstance: currentInstance}
}

func (s *Storage) Init(ctx context.Context, category, fileName string) error {
	peers, err := s.client.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("could not get peers from etcd %w", err)
	}
	for _, peer := range peers {
		if peer.Name == s.currentInstance {
			continue
		}
		if err := s.client.AddChunkToReplicationQueue(ctx, peer.Name, Chunk{
			Category: category,
			FileName: fileName,
			OwnedBy:  s.currentInstance,
		}); err != nil {
			return fmt.Errorf("could not send file to peer %s %w", peer.Name, err)
		}
	}
	return nil
}
