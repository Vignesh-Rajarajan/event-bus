package main

import (
	"flag"
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/Vignesh-Rajarajan/event-bus/web"
	"log"
	"os"
	"path/filepath"
)

var (
	filebased = flag.Bool("filebased", false, "Use file based event bus")
	dirname   = flag.String("dirname", "events.dat", "File name to use for file based event bus")
	port      = flag.Int("port", 8090, "Port to start the server on")
)

func main() {
	flag.Parse()
	var s *web.Server
	if *filebased {
		if *dirname == "" {
			log.Fatalf("the falg `--dirname` cannot be empty")
		}
		fileName := filepath.Join(*dirname, "events")
		fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("error creating a test in provided directory %s file %v", *dirname, err)
		}
		_ = fp.Close()
		_ = os.Remove(fp.Name())
		diskManager, err := manager.NewEventBusOnDisk(*dirname)
		if err != nil {
			log.Fatalf("error creating disk manager %v", err)
		}
		s = web.NewServer(diskManager, uint(*port))
	} else {
		s = web.NewServer(&manager.EventBusInMemory{}, uint(*port))
	}

	log.Default().Println("Starting server on port ", *port, " filebased ", *filebased, " dirname ", *dirname, " ...")
	if err := s.Start(); err != nil {
		log.Fatalf("Error starting server %v", err)
	}
}
