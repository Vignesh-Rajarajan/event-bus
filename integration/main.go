package integration

import (
	"github.com/Vignesh-Rajarajan/event-bus/web"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func InitAndServer(etcdAddr, dirname string, port int) error {
	cfg := clientv3.Config{
		Endpoints: strings.Split(etcdAddr, ","),
	}

	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		log.Fatalf("error creating etcd client %v", err)
	}
	etcdCli := clientv3.NewKV(etcdClient)
	fileName := filepath.Join(dirname, "events")
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error creating a test in provided directory %s file %v", dirname, err)
	}
	_ = fp.Close()
	_ = os.Remove(fp.Name())

	s := web.NewServer(etcdCli, dirname, uint(port))

	log.Default().Println("Starting server on port ", port, " dirname ", dirname, " ...")
	return s.Start()
}
