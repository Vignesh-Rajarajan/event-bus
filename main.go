package main

import (
	"flag"
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/Vignesh-Rajarajan/event-bus/web"
	"log"
)

var (
	filebased = flag.Bool("filebased", false, "Use file based event bus")
	filename  = flag.String("filename", "events.dat", "File name to use for file based event bus")
	port      = flag.Int("port", 8090, "Port to start the server on")
)

func main() {
	flag.Parse()
	var s *web.Server
	if *filebased {
		if *filename == "" {
			log.Fatal("Filename cannot be empty")
		}
		diskManager, err := manager.NewEventBusOnDisk(*filename)
		if err != nil {
			log.Fatalf("Error creating event bus %v", err)
		}
		s = web.NewServer(diskManager, uint(*port))
	} else {
		s = web.NewServer(&manager.EventBusInMemory{}, uint(*port))
	}

	log.Default().Println("Starting server on port ", *port, " filebased ", *filebased, " filename ", *filename, " ...")
	if err := s.Start(); err != nil {
		log.Fatalf("Error starting server %v", err)
	}
}
