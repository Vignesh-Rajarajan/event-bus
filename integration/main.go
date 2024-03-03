package integration

import (
	"context"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/replication"
	"github.com/Vignesh-Rajarajan/event-bus/web"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func InitAndServer(etcdAddr, dirname, instance, listenerAddr string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 5 * time.Second,
	}
	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		log.Fatalf("error creating etcd client %v", err)
	}
	defer etcdClient.Close()
	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return fmt.Errorf("error putting key in etcd %w", err)
	}
	_, err = etcdClient.Put(ctx, "peers/"+instance, listenerAddr)
	if err != nil {
		return fmt.Errorf("couldn't register peer address in etcd %w", err)
	}
	fileName := filepath.Join(dirname, "events")
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error creating a test in provided directory %s file %v", dirname, err)
	}
	_ = fp.Close()
	_ = os.Remove(fp.Name())

	s := web.NewServer(etcdClient, instance, dirname, listenerAddr, replication.NewStorage(etcdClient, instance))

	log.Default().Println("Starting server on addr ", listenerAddr, " dirname ", dirname, " ...", "etcd ", etcdAddr)
	return s.Start()
}
