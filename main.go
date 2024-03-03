package main

import (
	"flag"
	"github.com/Vignesh-Rajarajan/event-bus/integration"
	"log"
)

var (
	dirname      = flag.String("dirname", "/tmp", "File name to use for file based event bus")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "etcd address")
	instanceName = flag.String("instance-name", "", "unique instance name")
	listenAddr   = flag.String("listen-addr", "http://127.0.0.1:8080", "network listen address")
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

	if err := integration.InitAndServer(*etcdAddr, *dirname, *instanceName, *listenAddr); err != nil {
		log.Fatalf("error starting server %v", err)
	}
}
