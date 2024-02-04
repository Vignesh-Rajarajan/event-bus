package main

import (
	"flag"
	"github.com/Vignesh-Rajarajan/event-bus/integration"
	"log"
)

var (
	dirname  = flag.String("dirname", "events.dat", "File name to use for file based event bus")
	port     = flag.Int("port", 8090, "Port to start the server on")
	etcdAddr = flag.String("etcd", "localhost:2379", "etcd address")
)

func main() {
	flag.Parse()
	if *dirname == "" {
		log.Fatalf("dirname cannot be empty")
	}
	if *port <= 0 {
		log.Fatalf("port cannot be empty")
	}
	if *etcdAddr == "" {
		log.Fatalf("etcd address cannot be empty")
	}
	if err := integration.InitAndServer(*etcdAddr, *dirname, *port); err != nil {
		log.Fatalf("error starting server %v", err)
	}
}
