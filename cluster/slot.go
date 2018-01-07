package cluster

import (
	"fmt"
	"sync"
)

// SlotState captures the state of a slot.
type SlotState int

func (s SlotState) String() string {
	switch s {
	case SlotStateOffline:
		return "offline"
	case SlotStateOnline:
		return "online"
	case SlotStatePreMigration:
		return "pre-migration"
	case SlotStateInMigration:
		return "in-migration"
	default:
		return fmt.Sprintf("state(%d)", s)
	}
}

const (
	SlotStateOffline SlotState = iota
	SlotStateOnline
	SlotStatePreMigration
	SlotStateInMigration
)

type Slot struct {
	mu *sync.RWMutex
	co *sync.Cond

	ID        int
	state     SlotState
	group     Group
	fromGroup Group // The source group if the slot is in migration.
}

func NewSlot(id int, state SlotState, group, fromGroup Group) *Slot {
	mu := new(sync.RWMutex)
	co := sync.NewCond(mu.RLocker()) // co.L is the reader-lock part of mu.
	return &Slot{
		mu:        mu,
		co:        co,
		ID:        id,
		state:     state,
		group:     group,
		fromGroup: fromGroup,
	}
}

func (s *Slot) State() SlotState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *Slot) Group() Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.group
}

func (s *Slot) FromGroup() Group {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fromGroup
}

func (s *Slot) MarkOffline() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != SlotStateOnline {
		return fmt.Errorf("cannot change %s slot to offline", s.state)
	}

	s.state = SlotStateOffline
	s.group = nil
	s.fromGroup = nil

	return nil
}

func (s *Slot) MarkOnline(group Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.state {
	case SlotStateOffline, SlotStateInMigration:
		s.state = SlotStateOnline
		s.group = group
		s.fromGroup = nil
		return nil
	default:
		return fmt.Errorf("cannot change %s slot to online", s.state)
	}
}

func (s *Slot) MarkPreMigration(group Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != SlotStateOnline {
		return fmt.Errorf("cannot change %s slot to pre-migration", s.state)
	}

	s.state = SlotStatePreMigration
	s.fromGroup = s.group
	s.group = group

	return nil
}

func (s *Slot) MarkInMigration() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != SlotStatePreMigration {
		return fmt.Errorf("cannot change %s slot to in-migration", s.state)
	}

	s.state = SlotStateInMigration
	// Wake all the requests/goroutines that are being blocked in
	// pre-migration state.
	s.co.Broadcast()

	return nil
}

// GetWorkingGroups returns the group the slot belongs to, as well as the
// possible source group if the slot is in migration.
//
// If the slot is offline, an error will be returned.
func (s *Slot) GetWorkingGroups() (g, from Group, err error) {
	s.co.L.Lock()
	defer s.co.L.Unlock()

	switch s.state {
	case SlotStateOffline:
		return nil, nil, fmt.Errorf("slot is offline")
	case SlotStatePreMigration:
		// To handle the migration in a highly consistent manner, we
		// must wait until the state has been changed to in-migration
		// if it is pre-migration before.
		for s.state != SlotStateInMigration {
			s.co.Wait()
		}
		// Go to the in-migration case.
		fallthrough
	case SlotStateInMigration:
		from = s.fromGroup
	}

	return s.group, from, nil
}
