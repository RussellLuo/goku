package main

import (
	"time"

	"github.com/RussellLuo/goku/cluster"
	"github.com/RussellLuo/goku/common"
)

type Group interface {
	cluster.Group
	common.Inserter
	common.Deleter
	common.Selector
}

type Mapper interface {
	MapToSlot(key string) (*cluster.Slot, error)
}

type LWWSet struct {
	mapper Mapper
}

func NewLWWSet(mapper Mapper) *LWWSet {
	return &LWWSet{mapper: mapper}
}

func (l *LWWSet) Insert(key, member string, timestamp int64, ttl time.Duration) (bool, error) {
	slot, err := l.mapper.MapToSlot(key)
	if err != nil {
		return false, err
	}
	g := slot.Group().(Group)
	return g.Insert(slot.ID, key, member, timestamp, ttl)
}

func (l *LWWSet) Delete(key, member string, timestamp int64) (bool, error) {
	slot, err := l.mapper.MapToSlot(key)
	if err != nil {
		return false, err
	}
	g := slot.Group().(Group)
	return g.Delete(slot.ID, key, member, timestamp)
}

func (l *LWWSet) Select(key string, timestamp int64) ([]common.Element, error) {
	slot, err := l.mapper.MapToSlot(key)
	if err != nil {
		return nil, err
	}
	g := slot.Group().(Group)
	return g.Select(slot.ID, key, timestamp)
}
