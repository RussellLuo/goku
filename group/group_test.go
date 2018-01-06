package group_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/RussellLuo/goku/common"
	"github.com/RussellLuo/goku/group"
)

type mockServer struct {
	addr     string
	insertFn func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error)
	deleteFn func(slotID int, key, member string, timestamp int64) (bool, error)
	selectFn func(slotID int, key string, timestamp int64) ([]common.Element, error)
}

func (s *mockServer) Addr() string {
	return s.addr
}

func (s *mockServer) Insert(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
	if s.insertFn == nil {
		return false, nil
	}
	return s.insertFn(slotID, key, member, timestamp, ttl)
}

func (s *mockServer) Delete(slotID int, key, member string, timestamp int64) (bool, error) {
	if s.deleteFn == nil {
		return false, nil
	}
	return s.deleteFn(slotID, key, member, timestamp)
}

func (s *mockServer) Select(slotID int, key string, timestamp int64) ([]common.Element, error) {
	if s.selectFn == nil {
		return nil, nil
	}
	return s.selectFn(slotID, key, timestamp)
}

func TestGroup_Insert(t *testing.T) {
	slotID := 0
	key := "key"
	member := "member"
	ts := time.Now().UnixNano()
	ttl := 2 * time.Second
	groupID := 1

	type inType struct {
		servers      []group.Server
		writeQuorum  int
		readStrategy string
	}

	type wantType struct {
		updated bool
		err     error
	}

	cases := []struct {
		in   inType
		want wantType
	}{
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, nil
						},
					},
					&mockServer{
						addr: "server2",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, nil
						},
					},
				},
				writeQuorum: 2,
			},
			want: wantType{
				updated: false,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, nil
						},
					},
					&mockServer{
						addr: "server2",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, fmt.Errorf("fail to insert at server2")
						},
					},
				},
				writeQuorum: 2,
			},
			want: wantType{
				updated: false,
				err:     fmt.Errorf("no quorum (fail to insert at server2)"),
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return true, nil
						},
					},
					&mockServer{
						addr: "server2",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							time.Sleep(10 * time.Microsecond)
							return false, fmt.Errorf("fail to insert at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				updated: true,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							time.Sleep(10 * time.Microsecond)
							return true, nil
						},
					},
					&mockServer{
						addr: "server2",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, fmt.Errorf("fail to insert at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				updated: true,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, fmt.Errorf("fail to insert at server1")
						},
					},
					&mockServer{
						addr: "server2",
						insertFn: func(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
							return false, fmt.Errorf("fail to insert at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				updated: false,
				err:     fmt.Errorf("no quorum (fail to insert at server1; fail to insert at server2)"),
			},
		},
	}

	for i, c := range cases {
		g := group.NewGroup(groupID, c.in.servers, c.in.writeQuorum, c.in.readStrategy)
		updated, err := g.Insert(slotID, key, member, ts, ttl)
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if updated != c.want.updated {
			t.Errorf("[case %d] updated: got(%+v) != want(%+v)", i, updated, c.want.updated)
		}
	}
}

func TestGroup_Delete(t *testing.T) {
	slotID := 0
	key := "key"
	member := "member"
	ts := time.Now().UnixNano()
	groupID := 1

	type inType struct {
		servers      []group.Server
		writeQuorum  int
		readStrategy string
	}

	type wantType struct {
		deleted bool
		err     error
	}

	cases := []struct {
		in   inType
		want wantType
	}{
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, nil
						},
					},
					&mockServer{
						addr: "server2",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, nil
						},
					},
				},
				writeQuorum: 2,
			},
			want: wantType{
				deleted: false,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, nil
						},
					},
					&mockServer{
						addr: "server2",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, fmt.Errorf("fail to delete at server2")
						},
					},
				},
				writeQuorum: 2,
			},
			want: wantType{
				deleted: false,
				err:     fmt.Errorf("no quorum (fail to delete at server2)"),
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return true, nil
						},
					},
					&mockServer{
						addr: "server2",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							time.Sleep(10 * time.Microsecond)
							return false, fmt.Errorf("fail to delete at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				deleted: true,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							time.Sleep(10 * time.Microsecond)
							return true, nil
						},
					},
					&mockServer{
						addr: "server2",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, fmt.Errorf("fail to delete at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				deleted: true,
				err:     nil,
			},
		},
		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, fmt.Errorf("fail to delete at server1")
						},
					},
					&mockServer{
						addr: "server2",
						deleteFn: func(slotID int, key, member string, timestamp int64) (bool, error) {
							return false, fmt.Errorf("fail to delete at server2")
						},
					},
				},
				writeQuorum: 1,
			},
			want: wantType{
				deleted: false,
				err:     fmt.Errorf("no quorum (fail to delete at server1; fail to delete at server2)"),
			},
		},
	}

	for i, c := range cases {
		g := group.NewGroup(groupID, c.in.servers, c.in.writeQuorum, c.in.readStrategy)
		deleted, err := g.Delete(slotID, key, member, ts)
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if deleted != c.want.deleted {
			t.Errorf("[case %d] updated: got(%+v) != want(%+v)", i, deleted, c.want.deleted)
		}
	}
}

func TestGroup_Select(t *testing.T) {
	slotID := 0
	ts := time.Now().UnixNano()
	groupID := 1

	type inType struct {
		servers      []group.Server
		writeQuorum  int
		readStrategy string
	}

	type wantType struct {
		elements []common.Element
		err      error
	}

	cases := []struct {
		in   inType
		want wantType
	}{

		{
			in: inType{
				servers: []group.Server{
					&mockServer{
						addr: "server1",
						selectFn: func(slotID int, key string, timestamp int64) ([]common.Element, error) {
							return []common.Element{
								{Member: "member", Timestamp: ts, TTL: 2 * time.Second},
							}, nil
						},
					},
					&mockServer{
						addr: "server2",
						selectFn: func(slotID int, key string, timestamp int64) ([]common.Element, error) {
							return nil, nil
						},
					},
				},
			},
			want: wantType{
				elements: []common.Element{
					{Member: "member", Timestamp: ts, TTL: 2 * time.Second},
				},
				err: nil,
			},
		},
	}

	for i, c := range cases {
		g := group.NewGroup(groupID, c.in.servers, c.in.writeQuorum, c.in.readStrategy)
		elements, err := g.Select(slotID, "key1", ts)
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if !reflect.DeepEqual(elements, c.want.elements) {
			t.Errorf("[case %d] updated: got(%+v) != want(%+v)", i, elements, c.want.elements)
		}
	}
}
