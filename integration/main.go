package integration

import (
	"context"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/replication"
	"github.com/Vignesh-Rajarajan/event-bus/web"
	"log"
	"os"
	"path/filepath"
	"time"
)

type InitArgs struct {
	EtcdAddr     []string
	Dirname      string
	Instance     string
	ListenerAddr string
	ClusterName  string
}

func InitAndServer(args InitArgs) error {

	etcdCli, err := replication.NewClient(args.EtcdAddr, args.ClusterName)
	if err != nil {
		return fmt.Errorf("error creating etcd client %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := etcdCli.RegisterPeer(ctx, replication.Peer{
		Addr: args.ListenerAddr,
		Name: args.Instance,
	}); err != nil {
		return fmt.Errorf("error registering peer %v", err)
	}

	fileName := filepath.Join(args.Dirname, "events")
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error creating a test in provided directory %s file %v", args.Dirname, err)
	}
	_ = fp.Close()
	_ = os.Remove(fp.Name())

	s := web.NewServer(etcdCli, args.Instance, args.Dirname, args.ListenerAddr, replication.NewStorage(etcdCli, args.Instance))

	log.Default().Println("Starting server on addr ", args.ListenerAddr, " dirname ", args.Dirname, " ...", "etcd ", args.EtcdAddr)
	return s.Start()
}
