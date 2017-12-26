package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	//"log"

	"github.com/hashicorp/raft"
)

type fsm Cluster

// Apply applies a Raft log entry to the cluster metadata.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Errorf("failed to unmarshal command: %s", err.Error()))
	}
	//log.Printf("command: %+v", c)

	switch c.Op {
	case "add_group":
		return f.applyAddGroup(c.GroupID, c.Servers)
	case "del_group":
		return f.applyDelGroup(c.GroupID)
	case "assign_slots":
		return f.applyAssignSlots(c.GroupID, c.StartSlotID, c.StopSlotID)
	case "change_slot_state":
		return f.applyChangeSlotState(c.GroupID, c.StartSlotID, c.SlotState)
	default:
		panic(fmt.Errorf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the cluster metadata.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// Clone the slots.
	slots := make(map[int]slotSnapshot, SlotNum)
	for slotID, slot := range f.slots {
		slots[slotID] = slotSnapshot{
			State:       slot.State(),
			GroupID:     slot.Group().ID(),
			FromGroupID: slot.FromGroup().ID(),
		}
	}

	// Clone the groups.
	f.mu.RLock()
	groups := make(map[int]groupSnapshot, len(f.groups))
	for groupID, g := range f.groups {
		groups[groupID] = groupSnapshot{
			Servers: g.Servers(),
		}
	}
	f.mu.RUnlock()

	return &fsmSnapshot{Slots: slots, Groups: groups}, nil
}

// Restore stores the cluster metadata to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	var fs fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&fs); err != nil {
		return err
	}

	// Set the groups state from the snapshot,
	// no lock required according to the hashicorp/raft docs.
	groups := make(map[int]Group, len(fs.Groups))
	for i, s := range fs.Groups {
		groups[i] = f.newGroup(i, s.Servers)
	}
	f.groups = groups

	// Set the slots state from the snapshot,
	// no lock required according to the hashicorp/raft docs.
	slots := make(map[int]*Slot, len(fs.Slots))
	for i, s := range fs.Slots {
		slots[i] = NewSlot(i, s.State, groups[s.GroupID], groups[s.FromGroupID])
	}
	f.slots = slots

	return nil
}

func (f *fsm) getGroup(groupID int) (Group, error) {
	f.mu.RLock()
	g, ok := f.groups[groupID]
	f.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("group %d not found", groupID)
	}
	return g, nil
}

func (f *fsm) applyAddGroup(groupID int, servers []Server) interface{} {
	f.mu.Lock()
	f.groups[groupID] = f.newGroup(groupID, servers)
	f.mu.Unlock()
	return nil
}

func (f *fsm) applyDelGroup(groupID int) error {
	g, err := f.getGroup(groupID)
	if err != nil {
		return err
	}

	// Mark the state of all slots belong to the given group as offline.
	for _, slot := range f.slots {
		if slot.Group() == g {
			if err := slot.MarkOffline(); err != nil {
				return err
			}
		}
	}

	f.mu.Lock()
	delete(f.groups, groupID)
	f.mu.Unlock()
	return nil
}

func (f *fsm) applyAssignSlots(toGroupID, startSlotID, stopSlotID int) interface{} {
	toGroup, err := f.getGroup(toGroupID)
	if err != nil {
		return err
	}

	for slotID := startSlotID; slotID <= stopSlotID; slotID++ {
		slot := f.slots[slotID]
		if err := slot.MarkOnline(toGroup); err != nil {
			return err
		}
	}

	return nil
}

func (f *fsm) applyChangeSlotState(toGroupID, slotID int, toState SlotState) interface{} {
	toGroup, err := f.getGroup(toGroupID)
	if err != nil {
		return err
	}

	slot := f.slots[slotID]

	switch toState {
	case SlotStatePreMigration:
		return slot.MarkPreMigration(toGroup)
	case SlotStateInMigration:
		return slot.MarkInMigration()
	case SlotStateOnline:
		return slot.MarkOnline(toGroup)
	default:
		return fmt.Errorf("unrecognized slot state: %d", toState)
	}
}

type slotSnapshot struct {
	State       SlotState `json:"state,omitempty"`
	GroupID     int       `json:"group_id,omitempty"`
	FromGroupID int       `json:"from_group_id,omitempty"`
}

type groupSnapshot struct {
	Servers []Server `json:"servers,omitempty"`
}

type fsmSnapshot struct {
	Slots  map[int]slotSnapshot  `json:"slots,omitempty"`
	Groups map[int]groupSnapshot `json:"groups,omitempty"`
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
