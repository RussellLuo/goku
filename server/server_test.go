package server_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/RussellLuo/goku/common"
	"github.com/RussellLuo/goku/server"
)

func TestServer_Insert(t *testing.T) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	type inType struct {
		key       string
		member    string
		timestamp int64
		ttl       time.Duration
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
				key:       "key1",
				member:    "member1",
				timestamp: ts,
				ttl:       2 * time.Second,
			},
			want: wantType{
				updated: false,
				err:     nil,
			},
		},
		{
			in: inType{
				key:       "key1",
				member:    "member1",
				timestamp: ts,
				ttl:       2 * time.Second,
			},
			want: wantType{
				updated: true,
				err:     nil,
			},
		},
		{
			in: inType{
				key:       "key1",
				member:    "member2",
				timestamp: ts,
				ttl:       2 * time.Second,
			},
			want: wantType{
				updated: false,
				err:     nil,
			},
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			updated, err := s.Insert(0, c.in.key, c.in.member, c.in.timestamp, c.in.ttl)
			if !reflect.DeepEqual(updated, c.want.updated) {
				t.Errorf("updated: got(%+v) != want(%+v)", updated, c.want.updated)
			}
			if !reflect.DeepEqual(err, c.want.err) {
				t.Errorf("err: got(%+v) != want(%+v)", err, c.want.err)
			}
		})
	}
}

func TestServer_Delete(t *testing.T) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	type inType struct {
		key       string
		member    string
		timestamp int64
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
				key:       "key1",
				member:    "member1",
				timestamp: ts,
			},
			want: wantType{
				deleted: true,
				err:     nil,
			},
		},
		{
			in: inType{
				key:       "key1",
				member:    "member1",
				timestamp: ts,
			},
			want: wantType{
				deleted: false,
				err:     nil,
			},
		},
		{
			in: inType{
				key:       "key1",
				member:    "member2",
				timestamp: ts,
			},
			want: wantType{
				deleted: true,
				err:     nil,
			},
		},
	}

	// Setup
	s.Insert(0, "key1", "member1", ts, 2*time.Second)
	s.Insert(0, "key1", "member2", ts, 2*time.Second)

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			deleted, err := s.Delete(0, c.in.key, c.in.member, c.in.timestamp)
			if !reflect.DeepEqual(deleted, c.want.deleted) {
				t.Errorf("updated: got(%+v) != want(%+v)", deleted, c.want.deleted)
			}
			if !reflect.DeepEqual(err, c.want.err) {
				t.Errorf("err: got(%+v) != want(%+v)", err, c.want.err)
			}
		})
	}
}

func TestServer_Select(t *testing.T) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	type inType struct {
		key      string
		elements []common.Element
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
				key: "key1",
				elements: []common.Element{
					{
						Member:    "member1",
						Timestamp: ts,
						TTL:       2 * time.Second,
					},
				},
			},
			want: wantType{
				elements: []common.Element{
					{
						Member:    "member1",
						Timestamp: ts,
						TTL:       2 * time.Second,
					},
				},
				err: nil,
			},
		},
		{
			in: inType{
				key: "key1",
				elements: []common.Element{
					{
						Member:    "member1",
						Timestamp: ts,
						TTL:       2 * time.Second,
					},
					{
						Member:    "member2",
						Timestamp: ts,
						TTL:       5 * time.Nanosecond,
					},
				},
			},
			want: wantType{
				elements: []common.Element{
					{
						Member:    "member1",
						Timestamp: ts,
						TTL:       2 * time.Second,
					},
				},
				err: nil,
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			t.Parallel()

			for _, e := range c.in.elements {
				s.Insert(0, c.in.key, e.Member, e.Timestamp, e.TTL)
			}
			elements, err := s.Select(0, c.in.key, ts+int64(10*time.Nanosecond))
			if !reflect.DeepEqual(elements, c.want.elements) {
				t.Errorf("elements: got(%+v) != want(%+v)", elements, c.want.elements)
			}
			if !reflect.DeepEqual(err, c.want.err) {
				t.Errorf("err: got(%+v) != want(%+v)", err, c.want.err)
			}
		})
	}
}
