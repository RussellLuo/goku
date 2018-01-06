package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/armon/go-radix"

	"github.com/RussellLuo/goku/common"
)

type Element struct {
	Timestamp int64
	TTL       time.Duration
}

type Slot struct {
	Mu    sync.RWMutex
	Store *radix.Tree
}

type Server struct {
	mu    sync.RWMutex
	slots map[int]*Slot
}

func NewServer() *Server {
	return &Server{slots: make(map[int]*Slot)}
}

func (s *Server) StartX() {}

func (s *Server) Slot(slotID int) *Slot {
	s.mu.RLock()
	slot, ok := s.slots[slotID]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		slot, ok = s.slots[slotID]
		if !ok {
			slot = &Slot{Store: radix.New()}
			s.slots[slotID] = slot
		}
		s.mu.Unlock()
	}
	return slot
}

func (s *Server) Key(slotID int, key string) string {
	return fmt.Sprintf("%02d%s", slotID, key)
}

func (s *Server) KeyMember(slotID int, key, member string) string {
	return fmt.Sprintf("%02d%s%s", slotID, key, member)
}

func (s *Server) Insert(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error) {
	slot := s.Slot(slotID)
	k := s.KeyMember(slotID, key, member)

	slot.Mu.Lock()
	_, updated := slot.Store.Insert(k, Element{Timestamp: timestamp, TTL: ttl})
	slot.Mu.Unlock()

	return updated, nil
}

func (s *Server) Delete(slotID int, key, member string, timestamp int64) (bool, error) {
	slot := s.Slot(slotID)
	k := s.KeyMember(slotID, key, member)

	slot.Mu.Lock()
	_, deleted := slot.Store.Delete(k)
	slot.Mu.Unlock()

	return deleted, nil
}

func (s *Server) Select(slotID int, key string, timestamp int64) ([]common.Element, error) {
	slot := s.Slot(slotID)
	k := s.Key(slotID, key)

	var all []common.Element
	slot.Mu.RLock()
	slot.Store.WalkPrefix(k, func(s string, v interface{}) bool {
		member := s[len(k):]
		e := v.(Element)
		all = append(all, common.Element{Member: member, Timestamp: e.Timestamp, TTL: e.TTL})
		return false
	})
	slot.Mu.RUnlock()

	var alive []common.Element
	for _, e := range all {
		if e.Timestamp+e.TTL.Nanoseconds() <= timestamp {
			// If the element is expired, remove it from the set.
			slot.Mu.Lock()
			slot.Store.Delete(k + e.Member)
			slot.Mu.Unlock()
		} else {
			alive = append(alive, e)
		}
	}

	return alive, nil
}
