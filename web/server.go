package web

import (
	"encoding/json"
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/valyala/fasthttp"
	"log"
)

type Server struct {
	manager manager.EventManager
	port    uint
	logger  *log.Logger
}

func NewServer(manager manager.EventManager, port uint) *Server {
	return &Server{manager: manager, port: port, logger: log.Default()}
}

func (s *Server) Start() error {
	return fasthttp.ListenAndServe(fmt.Sprintf(":%d", s.port), s.handleRequest)
}

func (s *Server) handleRequest(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.handleSend(ctx)
	case "/read":
		s.handleReceive(ctx)
	case "/ack":
		s.ackHandler(ctx)
	case "/listChunks":
		s.listChunksHandler(ctx)
	default:
		s.logger.Println(fmt.Sprintf("path %s doesn't exist", ctx.Path()))
		ctx.Error("Unsupported path", fasthttp.StatusNotFound)
	}
}

func (s *Server) handleSend(ctx *fasthttp.RequestCtx) {
	if err := s.manager.Write(ctx.PostBody()); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}

func (s *Server) handleReceive(ctx *fasthttp.RequestCtx) {
	chunk := string(ctx.QueryArgs().Peek("chunk"))
	if chunk == "" {
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
	err = s.manager.Read(chunk, uint64(offset), uint64(maxSize), ctx)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	return
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
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
	if err := s.manager.Ack(chunk, uint64(size)); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) {
	chunks, err := s.manager.ListChunks()
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
