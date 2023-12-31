package main

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/client"
	"go/build"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	bufferSize = 1024 * 1024
	maxElem    = 10_000_000

	sendFmt = "Send: net %13s, cpu %13s, (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func main() {
	if err := runTests(); err != nil {
		log.Fatalf("Error running tests %v", err)
	}

}
func runTests() error {
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}

	log.Default().Printf("compiling project")

	err := exec.Command("go", "install", "-v", "github.com/Vignesh-Rajarajan/event-bus").Run()
	if err != nil {
		return fmt.Errorf("error compiling project %v", err)
	}

	port := rand.Int()%1000 + 8000

	dbDirname := "/tmp/events/"
	if err = os.RemoveAll(dbDirname); err != nil {
		log.Default().Println(fmt.Errorf("error removing file %q, %v", dbDirname, err))
	}
	_ = os.Mkdir(dbDirname, 0777)

	_ = os.WriteFile(dbDirname+"chunk1", []byte("12345\n"), 0666)
	cmd := exec.Command(goPath+"/bin/event-bus", "-filebased", "-dirname", dbDirname, "-port", strconv.Itoa(port))
	if err = cmd.Start(); err != nil {
		return err
	}
	defer cmd.Process.Kill()

	log.Default().Printf("starting server on port %d", port)

	// wait for server to start
	for {
		timeout := time.Millisecond * 100
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			continue
		}
		_ = conn.Close()
		break
	}
	log.Default().Printf("testing started")
	c := client.NewClient(fmt.Sprintf("http://localhost:%d", port))
	want, err := send(c)
	if err != nil {
		return err
	}

	got, err := receive(c)
	if err != nil {
		return fmt.Errorf("receieve error %v", err)
	}
	want += 12345
	if strconv.FormatInt(want, 10) != strconv.FormatInt(got, 10) {
		return fmt.Errorf("error : want %v got %v delivered %1.f%%", want, got, float64(got)/float64(want)*100)
	}
	log.Default().Printf("Success %d %d", want, got)
	return nil
}

func send(c *client.Client) (sum int64, err error) {
	start := time.Now()
	var networkTime time.Duration
	var sentBytes int64
	defer func() {
		log.Default().Printf(sendFmt, networkTime, time.Since(start)-networkTime, float64(sentBytes)/1024/1024)
	}()
	buff := make([]byte, 0, bufferSize)

	for i := 0; i <= maxElem; i++ {
		sum += int64(i)
		buff = strconv.AppendInt(buff, int64(i), 10)
		buff = append(buff, '\n')

		if len(buff) >= bufferSize {
			networkStart := time.Now()
			if err := c.Send(buff); err != nil {
				return 0, err
			}
			networkTime += time.Since(networkStart)
			sentBytes += int64(len(buff))
			buff = buff[0:0]
		}
	}
	if len(buff) > 0 {
		networkStart := time.Now()
		if err := c.Send(buff); err != nil {
			return 0, err
		}
		networkTime += time.Since(networkStart)
		sentBytes += int64(len(buff))

	}
	return sum, nil
}

func receive(c *client.Client) (sum int64, err error) {
	buff := make([]byte, bufferSize)

	var parseTime time.Duration
	recvStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(recvStart)-parseTime, parseTime)
	}()

	trimNewLine := func(r rune) bool {
		return r == '\n'
	}

	for {
		res, err := c.Receive(buff)
		if err == io.EOF {
			return sum, nil
		}
		if err != nil {
			return 0, err
		}
		start := time.Now()
		ints := strings.Split(strings.TrimRightFunc(string(res), trimNewLine), "\n")
		for _, str := range ints {
			if str == "" {
				continue
			}
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}
			sum += int64(i)
		}
		parseTime += time.Since(start)
	}
}
