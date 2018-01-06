package http

import (
	"io"
	"net"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/RussellLuo/goku/cmd/goku-proxy/pb"
)

var (
	marshaler   = &jsonpb.Marshaler{EnumsAsInts: true, EmitDefaults: true}
	unmarshaler = &jsonpb.Unmarshaler{}
)

type Method func(context.Context, proto.Message) (proto.Message, error)

func MakeHandler(method Method, in proto.Message) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if err := unmarshaler.Unmarshal(r.Body, in); err != nil {
			if err != io.EOF {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		out, err := method(nil, in)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := marshaler.Marshal(w, out); err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

type GokuProxy struct {
	srv         pb.GokuProxyServer
	interceptor grpc.UnaryServerInterceptor
}

func NewGokuProxy(srv pb.GokuProxyServer, interceptor grpc.UnaryServerInterceptor) *GokuProxy {
	return &GokuProxy{srv: srv, interceptor: interceptor}
}

func (g *GokuProxy) HandlerMap() map[string]http.HandlerFunc {
	m := make(map[string]http.HandlerFunc)
	m["/goku_proxy/add_group"] = MakeHandler(g.AddGroup, new(pb.AddGroupRequest))
	m["/goku_proxy/del_group"] = MakeHandler(g.DelGroup, new(pb.DelGroupRequest))
	m["/goku_proxy/assign_slots"] = MakeHandler(g.AssignSlots, new(pb.AssignSlotsRequest))
	m["/goku_proxy/insert"] = MakeHandler(g.Insert, new(pb.InsertRequest))
	m["/goku_proxy/delete"] = MakeHandler(g.Delete, new(pb.DeleteRequest))
	m["/goku_proxy/select"] = MakeHandler(g.Select, new(pb.SelectRequest))
	return m
}

func (g *GokuProxy) AddGroup(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.AddGroup(ctx, in.(*pb.AddGroupRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.AddGroupRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/AddGroup",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.AddGroup(ctx, req.(*pb.AddGroupRequest))
		},
	)
	return out.(*pb.AddGroupReply), err
}

func (g *GokuProxy) DelGroup(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.DelGroup(ctx, in.(*pb.DelGroupRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.DelGroupRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/DelGroup",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.DelGroup(ctx, req.(*pb.DelGroupRequest))
		},
	)
	return out.(*pb.DelGroupReply), err
}

func (g *GokuProxy) AssignSlots(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.AssignSlots(ctx, in.(*pb.AssignSlotsRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.AssignSlotsRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/AssignSlots",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.AssignSlots(ctx, req.(*pb.AssignSlotsRequest))
		},
	)
	return out.(*pb.AssignSlotsReply), err
}

func (g *GokuProxy) Insert(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.Insert(ctx, in.(*pb.InsertRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.InsertRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/Insert",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.Insert(ctx, req.(*pb.InsertRequest))
		},
	)
	return out.(*pb.InsertReply), err
}

func (g *GokuProxy) Delete(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.Delete(ctx, in.(*pb.DeleteRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.DeleteRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/Delete",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.Delete(ctx, req.(*pb.DeleteRequest))
		},
	)
	return out.(*pb.DeleteReply), err
}

func (g *GokuProxy) Select(ctx context.Context, in proto.Message) (proto.Message, error) {
	if g.interceptor == nil {
		return g.srv.Select(ctx, in.(*pb.SelectRequest))
	}
	out, err := g.interceptor(
		ctx,
		in.(*pb.SelectRequest),
		&grpc.UnaryServerInfo{
			Server:     g.srv,
			FullMethod: "/pb.GokuProxy/Select",
		},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return g.srv.Select(ctx, req.(*pb.SelectRequest))
		},
	)
	return out.(*pb.SelectReply), err
}

type Server struct {
	mux         *http.ServeMux
	interceptor grpc.UnaryServerInterceptor
}

func NewServer(interceptors ...grpc.UnaryServerInterceptor) *Server {
	var interceptor grpc.UnaryServerInterceptor
	switch len(interceptors) {
	case 0:
	case 1:
		interceptor = interceptors[0]
	default:
		panic("At most one unary server interceptor can be set.")
	}

	return &Server{
		mux:         http.NewServeMux(),
		interceptor: interceptor,
	}
}

func (s *Server) RegisterGokuProxyServer(srvGokuProxy pb.GokuProxyServer) {
	for pattern, handler := range NewGokuProxy(srvGokuProxy, s.interceptor).HandlerMap() {
		s.mux.Handle(pattern, handler)
	}
}

func (s *Server) Serve(l net.Listener) error {
	server := &http.Server{Handler: s.mux}
	return server.Serve(l)
}
