package main

import (
	"log"
	"net"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"github.com/RussellLuo/goku/cmd/goku-server/http"
	"github.com/RussellLuo/goku/cmd/goku-server/pb"
	"github.com/RussellLuo/goku/server"
)

func serve(srv pb.GokuServerServer, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	m := cmux.New(lis)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldPrefixSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	httpS := http.NewServer()
	httpS.RegisterGokuServerServer(srv)
	go func() {
		if err := httpS.Serve(httpL); err != nil {
			log.Fatalf("failed to start HTTP server listening: %v", err)
		}
	}()

	grpcS := grpc.NewServer()
	pb.RegisterGokuServerServer(grpcS, srv)
	go func() {
		if err := grpcS.Serve(grpcL); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	return m.Serve()
}

func main() {
	// TODO: Read arguments from flags.
	proxyAddr := ":50052"

	s := NewServer(server.NewServer())
	if err := serve(s, proxyAddr); err != nil {
		log.Fatalf("err: %v", err)
	}
}
