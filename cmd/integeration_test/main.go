package main

import (
	"errors"
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

type testResult struct {
	sum int64
	err error
}

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
	want, got, err := sendAndReceive(c)
	if err != nil {
		return err
	}
	want += 12345
	if want != got {
		return fmt.Errorf("error : want %v got %v delivered %1.f%%", want, got, float64(got)/float64(want)*100)
	}
	log.Default().Printf("Success %d %d", want, got)
	return nil
}

func sendAndReceive(c *client.Client) (want, got int64, err error) {
	wantChan := make(chan testResult, 1)
	gotChan := make(chan testResult, 1)
	sendCompleted := make(chan bool, 1)
	go func() {
		want, err := send(c)
		log.Default().Printf("send completed")
		wantChan <- testResult{sum: want, err: err}
		sendCompleted <- true
	}()

	go func() {
		got, err := receive(c, sendCompleted)
		log.Default().Printf("receive completed")
		gotChan <- testResult{sum: got, err: err}
	}()
	wantRes := <-wantChan
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("sendAndReceive error while sending %v", wantRes.err)
	}
	gotRes := <-gotChan
	if gotRes.err != nil {
		return 0, 0, fmt.Errorf("sendAndReceive error while receiving %v", gotRes.err)
	}
	return wantRes.sum, gotRes.sum, nil
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

func receive(c *client.Client, sendCompleted chan bool) (sum int64, err error) {
	buff := make([]byte, bufferSize)
	var parseTime time.Duration
	recvStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(recvStart)-parseTime, parseTime)
	}()

	trimNewLine := func(r rune) bool {
		return r == '\n'
	}
	sendFinished := false
	for {
		select {
		case <-sendCompleted:
			log.Default().Printf("Receive: send completed")
			sendFinished = true
		default:

		}
		res, err := c.Receive(buff)
		if errors.Is(err, io.EOF) {
			if sendFinished {
				return sum, nil
			}
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if err != nil {
			log.Default().Printf("Recieve: err")
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
