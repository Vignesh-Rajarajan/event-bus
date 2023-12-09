package web

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/event-bus/manager"
	"github.com/valyala/fasthttp"
	"log"
)

type Server struct {
	manager manager.EventManager
	port    uint
}

func NewServer(manager manager.EventManager, port uint) *Server {
	return &Server{manager: manager, port: port}
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
	default:
		log.Default().Println(fmt.Sprintf("path %s doesn't exist", ctx.Path()))
		ctx.Error("Unsupported path", fasthttp.StatusNotFound)
	}
}

func (s *Server) handleSend(ctx *fasthttp.RequestCtx) {
	if err := s.manager.Write(ctx.PostBody()); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}

func (s *Server) handleReceive(ctx *fasthttp.RequestCtx) {
	offset, err := ctx.QueryArgs().GetUint("offset")
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	log.Printf("Receive called with offset %d maxSize %d", offset, maxSize)
	err = s.manager.Read(uint64(offset), uint64(maxSize), ctx)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	return
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	if err := s.manager.Ack(); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
	}
}
