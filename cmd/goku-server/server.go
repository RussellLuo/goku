package main

import (
	"time"

	"golang.org/x/net/context"

	"github.com/RussellLuo/goku/cmd/goku-server/pb"
	"github.com/RussellLuo/goku/server"
)

type Server struct {
	server *server.Server
}

func NewServer(server *server.Server) *Server {
	return &Server{server: server}
}

func (s *Server) Insert(ctx context.Context, in *pb.InsertRequest) (*pb.InsertReply, error) {
	updated, err := s.server.Insert(int(in.SlotId), in.Key, in.Member, in.TimestampNs, time.Duration(in.TtlNs))

	out := &pb.InsertReply{Updated: updated}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (s *Server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteReply, error) {
	deleted, err := s.server.Delete(int(in.SlotId), in.Key, in.Member, in.TimestampNs)

	out := &pb.DeleteReply{Deleted: deleted}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (s *Server) Select(ctx context.Context, in *pb.SelectRequest) (*pb.SelectReply, error) {
	elements, err := s.server.Select(int(in.SlotId), in.Key, in.TimestampNs)

	out := &pb.SelectReply{}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	} else {
		out.Elements = make([]*pb.Element, len(elements))
		for i, e := range elements {
			out.Elements[i] = &pb.Element{
				Member:      e.Member,
				TimestampNs: e.Timestamp,
				TtlNs:       int64(e.TTL),
			}
		}
	}
	return out, nil
}
