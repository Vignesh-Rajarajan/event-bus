package integration

import (
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/Vignesh-Rajarajan/event-bus/web"
	"log"
	"os"
	"path/filepath"
)

func InitAndServer(dirname string, port int) error {
	fileName := filepath.Join(dirname, "events")
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error creating a test in provided directory %s file %v", dirname, err)
	}
	_ = fp.Close()
	_ = os.Remove(fp.Name())
	diskManager, err := manager.NewEventBusOnDisk(dirname)
	if err != nil {
		log.Fatalf("error creating disk manager %v", err)
	}
	s := web.NewServer(diskManager, uint(port))

	log.Default().Println("Starting server on port ", port, " dirname ", dirname, " ...")
	return s.Start()
}
