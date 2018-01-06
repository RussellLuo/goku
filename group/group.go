package group

import (
	"fmt"
	"strings"
	"time"

	"github.com/RussellLuo/goku/cluster"
	"github.com/RussellLuo/goku/common"
)

type Server interface {
	common.Inserter
	common.Deleter
	common.Selector

	Addr() string
}

type group struct {
	id           int
	servers      []Server
	writeQuorum  int
	readStrategy string
}

func NewGroup(id int, servers []Server, writeQuorum int, readStrategy string) *group {
	return &group{
		id:           id,
		servers:      servers,
		writeQuorum:  writeQuorum,
		readStrategy: readStrategy,
	}
}

func (g *group) ID() int {
	return g.id
}

func (g *group) Servers() []cluster.Server {
	addrs := make([]cluster.Server, len(g.servers))
	for i, s := range g.servers {
		addrs[i] = cluster.Server(s.Addr())
	}
	return addrs
}

func (g *group) write(action func(s Server) (bool, error)) (bool, error) {
	type result struct {
		status bool
		err    error
	}
	// Scatter
	resultChan := make(chan result, len(g.servers))
	for _, s := range g.servers {
		go func(s Server) {
			status, err := action(s)
			resultChan <- result{status: status, err: err}
		}(s)
	}

	// Gather
	var (
		status     = false
		errs       = []string(nil)
		got        = 0
		need       = g.writeQuorum
		haveQuorum = func() bool { return (got - len(errs)) >= need }
	)
	for i := 0; i < cap(resultChan); i++ {
		result := <-resultChan
		status = result.status
		if result.err != nil {
			errs = append(errs, result.err.Error())
		}

		got++
		if haveQuorum() {
			break
		}
	}

	// Report
	if !haveQuorum() {
		return false, fmt.Errorf("no quorum (%s)", strings.Join(errs, "; "))
	}

	return status, nil
}

func (g *group) Insert(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
	return g.write(func(s Server) (bool, error) {
		return s.Insert(slotID, key, member, timestamp, ttl)
	})
}

func (g *group) Delete(slotID int, key, member string, timestamp int64) (bool, error) {
	return g.write(func(s Server) (bool, error) {
		return s.Delete(slotID, key, member, timestamp)
	})
}

func (g *group) Select(slotID int, key string, timestamp int64) ([]common.Element, error) {
	// TODO: select according to g.readStrategy
	return g.servers[0].Select(slotID, key, timestamp)
}

func (g *group) MigrateKeys(to cluster.Group, slotID int, keys ...string) error {
	return nil
}

func (g *group) MigrateSlot(to cluster.Group, slotID int) error {
	return nil
}
