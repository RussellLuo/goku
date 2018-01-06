package main

import (
	"log"
	"net"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"github.com/RussellLuo/goku/cluster"
	"github.com/RussellLuo/goku/cmd/goku-proxy/http"
	"github.com/RussellLuo/goku/cmd/goku-proxy/pb"
	"github.com/RussellLuo/goku/group"
)

func serve(srv pb.GokuProxyServer, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	m := cmux.New(lis)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldPrefixSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	httpS := http.NewServer()
	httpS.RegisterGokuProxyServer(srv)
	go func() {
		if err := httpS.Serve(httpL); err != nil {
			log.Fatalf("failed to start HTTP server listening: %v", err)
		}
	}()

	grpcS := grpc.NewServer()
	pb.RegisterGokuProxyServer(grpcS, srv)
	go func() {
		if err := grpcS.Serve(grpcL); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	return m.Serve()
}

func main() {
	// TODO: Read arguments from flags.
	writeQuorum := 1
	readStrategy := ""
	timeout := 2 * time.Second
	raftBind := "127.0.0.1:12000"
	raftDir := "~/node"
	proxyAddr := ":50051"

	newGroup := func(id int, serverAddrs []cluster.Server) cluster.Group {
		servers := make([]group.Server, len(serverAddrs))
		for i, sAddr := range serverAddrs {
			addr := string(sAddr)
			servers[i] = group.NewServer(addr, timeout, group.NewPool(addr))
		}
		return group.NewGroup(id, servers, writeQuorum, readStrategy)
	}

	c := cluster.NewCluster("goku-proxy", newGroup, raftBind, raftDir)
	if err := c.Open(true, "node"); err != nil {
		panic(err)
	}

	l := NewLWWSet(c)

	proxy := NewProxy(c, l)
	if err := serve(proxy, proxyAddr); err != nil {
		log.Fatalf("err: %v", err)
	}
}
