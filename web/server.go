package web

import (
	"encoding/json"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Server struct {
	etcd     clientv3.KV
	dirname  string
	m        sync.Mutex
	storages map[string]manager.EventManager
	port     uint
	logger   *log.Logger
}

func NewServer(etcd clientv3.KV, dirname string, port uint) *Server {
	return &Server{etcd: etcd, dirname: dirname, port: port, logger: log.Default(), storages: make(map[string]manager.EventManager)}
}

func (s *Server) Start() error {
	return fasthttp.ListenAndServe(fmt.Sprintf(":%d", s.port), s.handleRequest)
}

func isValidCategory(category string) bool {
	if category == "" {
		return false
	}
	cleanPath := filepath.Clean(category)
	if cleanPath != category {
		return false
	}
	if strings.ContainsAny(cleanPath, `/\.`) {
		return false
	}
	return true
}

func (s *Server) getStorage(category string) (manager.EventManager, error) {
	s.m.Lock()
	defer s.m.Unlock()
	if !isValidCategory(category) {
		return nil, fmt.Errorf("invalid category %s", category)
	}
	storage, ok := s.storages[category]
	if ok {
		return storage, nil
	}
	dir := filepath.Join(s.dirname, category)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("error creating directory %s: %v", dir, err)
	}
	storage, err := manager.NewEventBusOnDisk(dir)
	if err != nil {
		return nil, fmt.Errorf("error creating storage: %v", err)
	}
	s.storages[category] = storage
	return storage, nil
}

func (s *Server) handleRequest(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.handleWrite(ctx)
	case "/read":
		s.handleRead(ctx)
	case "/ack":
		s.ackHandler(ctx)
	case "/listChunks":
		s.listChunksHandler(ctx)
	default:
		s.logger.Println(fmt.Sprintf("path %s doesn't exist", ctx.Path()))
		ctx.Error("Unsupported path", fasthttp.StatusNotFound)
	}
}

func (s *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	category := string(ctx.QueryArgs().Peek("category"))
	if category == "" {
		ctx.Error("category cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	storage, err := s.getStorage(category)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	if err := storage.Write(ctx.PostBody()); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}

func (s *Server) handleRead(ctx *fasthttp.RequestCtx) {
	category := string(ctx.QueryArgs().Peek("category"))
	if category == "" {
		ctx.Error("category cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	storage, err := s.getStorage(category)
	if err != nil {
		fmt.Println(fmt.Sprintf("error getting storage %v", err))
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	chunk := string(ctx.QueryArgs().Peek("chunk"))
	if chunk == "" {
		fmt.Println(fmt.Sprintf("chunk cannot be empty"))
		ctx.Error("chunk cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	offset, err := ctx.QueryArgs().GetUint("offset")
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	err = storage.Read(chunk, uint64(offset), uint64(maxSize), ctx)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	return
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	category := string(ctx.QueryArgs().Peek("category"))
	if category == "" {
		ctx.Error("category cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	storage, err := s.getStorage(category)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	chunk := string(ctx.QueryArgs().Peek("chunk"))
	if chunk == "" {
		ctx.Error("chunk cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.Error(fmt.Sprintf("bad `size` getParam: %v", err.Error()), fasthttp.StatusBadRequest)
		return
	}
	if err := storage.Ack(chunk, uint64(size)); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) {
	category := string(ctx.QueryArgs().Peek("category"))
	if category == "" {
		ctx.Error("category cannot be empty", fasthttp.StatusBadRequest)
		return
	}
	storage, err := s.getStorage(category)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	chunks, err := storage.ListChunks()
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(ctx).Encode(chunks)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
}
