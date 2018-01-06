package main

import (
	"time"

	"golang.org/x/net/context"

	"github.com/RussellLuo/goku/cluster"
	"github.com/RussellLuo/goku/cmd/goku-proxy/pb"
)

type Proxy struct {
	cluster *cluster.Cluster
	lwwset  *LWWSet
}

func NewProxy(cluster *cluster.Cluster, lwwset *LWWSet) *Proxy {
	return &Proxy{
		cluster: cluster,
		lwwset:  lwwset,
	}
}

func (p *Proxy) AddGroup(ctx context.Context, in *pb.AddGroupRequest) (*pb.AddGroupReply, error) {
	servers := make([]cluster.Server, len(in.Servers))
	for i, s := range in.Servers {
		servers[i] = cluster.Server(s)
	}

	err := p.cluster.AddGroup(int(in.GroupId), servers...)

	out := &pb.AddGroupReply{}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (p *Proxy) DelGroup(ctx context.Context, in *pb.DelGroupRequest) (*pb.DelGroupReply, error) {
	err := p.cluster.DelGroup(int(in.GroupId))

	out := &pb.DelGroupReply{}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (p *Proxy) AssignSlots(ctx context.Context, in *pb.AssignSlotsRequest) (*pb.AssignSlotsReply, error) {
	err := p.cluster.AssignSlots(int(in.ToGroupId), int(in.StartSlotId), int(in.StopSlotId))

	out := &pb.AssignSlotsReply{}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (p *Proxy) Insert(ctx context.Context, in *pb.InsertRequest) (*pb.InsertReply, error) {
	updated, err := p.lwwset.Insert(in.Key, in.Member, in.TimestampNs, time.Duration(in.TtlNs))

	out := &pb.InsertReply{Updated: updated}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (p *Proxy) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteReply, error) {
	deleted, err := p.lwwset.Delete(in.Key, in.Member, in.TimestampNs)

	out := &pb.DeleteReply{Deleted: deleted}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	}
	return out, nil
}

func (p *Proxy) Select(ctx context.Context, in *pb.SelectRequest) (*pb.SelectReply, error) {
	elements, err := p.lwwset.Select(in.Key, in.TimestampNs)

	out := &pb.SelectReply{}
	if err != nil {
		out.Error = &pb.Error{Message: err.Error()}
	} else {
		out.Elements = make([]*pb.Element, len(elements))
		for i, e := range elements {
			out.Elements[i] = &pb.Element{
				Member: e.Member,
				TimestampNs: e.Timestamp,
				TtlNs: int64(e.TTL),
			}
		}
	}
	return out, nil
}
