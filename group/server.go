package group

import (
	"context"
	"fmt"
	"time"

	"github.com/RussellLuo/goku/common"
	"github.com/RussellLuo/goku/group/pb"
)

type Pool interface {
	Get() (pb.GokuServerClient, error)
	Put(cli pb.GokuServerClient)
	CloseAll() error
}

type server struct {
	addr    string
	timeout time.Duration
	pool    Pool
}

func NewServer(addr string, timeout time.Duration, pool Pool) *server {
	return &server{
		addr:    addr,
		timeout: timeout,
		pool:    pool,
	}
}

func (s *server) Addr() string {
	return s.addr
}

func (s *server) Insert(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.timeout)
	defer cancelFunc()

	cli, err := s.pool.Get()
	if err != nil {
		return false, err
	}
	defer s.pool.Put(cli)

	reply, err := cli.Insert(ctx, &pb.InsertRequest{
		SlotId:    int64(slotID),
		Key:       key,
		Member:    member,
		TimestampNs: timestamp,
		TtlNs:     ttl.Nanoseconds(),
	})
	if err != nil {
		return false, err
	}

	if reply.Error != nil {
		err = fmt.Errorf(reply.Error.Message)
	}
	return reply.Updated, err
}

func (s *server) Delete(slotID int, key, member string, timestamp int64) (bool, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.timeout)
	defer cancelFunc()

	cli, err := s.pool.Get()
	if err != nil {
		return false, err
	}
	defer s.pool.Put(cli)

	reply, err := cli.Delete(ctx, &pb.DeleteRequest{
		SlotId:    int64(slotID),
		Key:       key,
		Member:    member,
		TimestampNs: timestamp,
	})
	if err != nil {
		return false, err
	}

	if reply.Error != nil {
		err = fmt.Errorf(reply.Error.Message)
	}
	return reply.Deleted, err
}

func (s *server) Select(slotID int, key string, timestamp int64) ([]common.Element, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), s.timeout)
	defer cancelFunc()

	cli, err := s.pool.Get()
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(cli)

	reply, err := cli.Select(ctx, &pb.SelectRequest{
		SlotId:    int64(slotID),
		Key:       key,
		TimestampNs: timestamp,
	})
	if err != nil {
		return nil, err
	}

	if reply.Error != nil {
		err = fmt.Errorf(reply.Error.Message)
	}
	elements := make([]common.Element, len(reply.Elements))
	for i, e := range reply.Elements {
		elements[i] = common.Element{
			Member:    e.Member,
			Timestamp: e.TimestampNs,
			TTL:       time.Duration(e.TtlNs),
		}
	}
	return elements, err
}
