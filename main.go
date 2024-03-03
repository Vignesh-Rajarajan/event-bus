package main

import (
	"flag"
	"github.com/Vignesh-Rajarajan/event-bus/integration"
	"log"
	"strings"
)

var (
	dirname      = flag.String("dirname", "/tmp", "File name to use for file based event bus")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "etcd address")
	instanceName = flag.String("instance", "op", "unique instance name")
	listenAddr   = flag.String("listen", "127.0.0.1:8080", "network listen address")
	clusterName  = flag.String("cluster", "default", "cluster name")
)

func main() {
	flag.Parse()
	if *dirname == "" {
		log.Fatalf("dirname cannot be empty")
	}
	if *instanceName == "" {
		log.Fatalf("instance name cannot be empty")
	}
	if *listenAddr == "" {
		log.Fatalf("listen address cannot be empty")
	}
	if *etcdAddr == "" {
		log.Fatalf("etcd address cannot be empty")
	}
	if *clusterName == "" {
		log.Fatalf("cluster name cannot be empty")

	}

	if err := integration.InitAndServer(integration.InitArgs{
		EtcdAddr:     strings.Split(*etcdAddr, ","),
		Dirname:      *dirname,
		Instance:     *instanceName,
		ListenerAddr: *listenAddr,
		ClusterName:  *clusterName,
	}); err != nil {
		log.Fatalf("error starting server %v", err)
	}
}
