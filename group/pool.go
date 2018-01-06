package group

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/RussellLuo/goku/group/pb"
)

type pool struct {
	mu   sync.Mutex
	addr string
	cli  pb.GokuServerClient
}

func NewPool(addr string) *pool {
	return &pool{addr: addr}
}

func (p *pool) Get() (pb.GokuServerClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cli == nil {
		conn, err := grpc.Dial(p.addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		p.cli = pb.NewGokuServerClient(conn)
	}

	return p.cli, nil
}

func (p *pool) Put(cli pb.GokuServerClient) {
	// TODO: actually put back client.
}

func (p *pool) CloseAll() error {
	// TODO: actually close all clients.
	return nil
}
