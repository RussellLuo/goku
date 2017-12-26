package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	SlotNum = 1024

	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")
)

type command struct {
	Op          string    `json:"op,omitempty"`
	GroupID     int       `json:"group_id,omitempty"`
	Servers     []Server  `json:"servers,omitempty"`
	StartSlotID int       `json:"start_slot_id,omitempty"`
	StopSlotID  int       `json:"stop_slot_id,omitempty"`
	SlotState   SlotState `json:"slot_state,omitempty"`
}

// Cluster is a cluster metadata manager, which manages the cluster
// metadata in a highly consistent manner by using the Raft protocol.
//
// The cluster metadata here represents a storage cluster that consists of
// many servers, which are divided into some groups. The whole data space
// is split into 1024 slots (a.k.a. shards), and different groups manage
// different slots.
type Cluster struct {
	name  string
	slots map[int]*Slot

	newGroup NewGroup
	mu       sync.RWMutex
	groups   map[int]Group

	// The consensus mechanism
	raft     *raft.Raft
	raftBind string
	raftDir  string
}

// NewCluster creates a Cluster with the given configurations.
func NewCluster(name string, newGroup NewGroup, raftBind, raftDir string) *Cluster {
	slots := make(map[int]*Slot, SlotNum)
	for i := 0; i < SlotNum; i++ {
		slots[i] = NewSlot(i, SlotStateOffline, nil, nil)
	}
	return &Cluster{
		name:     name,
		slots:    slots,
		newGroup: newGroup,
		groups:   make(map[int]Group),
		raftBind: raftBind,
		raftDir:  raftDir,
	}
}

// Open opens the cluster. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (c *Cluster) Open(enableSingle bool, localID string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", c.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(c.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(c.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(c.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(c), logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	c.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// Close closes the cluster. If wait is true, waits for a graceful shutdown.
func (c *Cluster) Close(wait bool) error {
	f := c.raft.Shutdown()
	if wait {
		if err := f.Error(); err != nil {
			return err
		}
	}

	return nil
}

// Join joins a node, identified by nodeID and located at addr, to this cluster.
// The node must be ready to respond to Raft communications at that address.
func (c *Cluster) Join(nodeID, addr string) error {
	log.Printf("received request to join node %s at %s", nodeID, addr)
	if c.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := f.Error(); err != nil {
		return err
	}
	log.Printf("node %s at %s joined successfully", nodeID, addr)

	return nil
}

func (c *Cluster) Name() string { return c.name }

// Slots returns the slots with the given slot ids. If no id is given,
// it will return all slots in the cluster.
//
// The slots are returned as a map, callers must not to modify the map.
func (c *Cluster) Slots(ids ...int) map[int]*Slot {
	if len(ids) == 0 {
		return c.slots
	}

	parts := make(map[int]*Slot, len(ids))
	for _, id := range ids {
		parts[id] = c.slots[id]
	}
	return parts
}

// Groups returns the groups with the given group ids. If no id is given,
// it will return all existing groups in the cluster.
//
// The groups are returned as a map, callers must not to modify the map.
func (c *Cluster) Groups(ids ...int) map[int]Group {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(ids) == 0 {
		return c.groups
	}

	parts := make(map[int]Group, len(ids))
	for _, id := range ids {
		parts[id] = c.groups[id]
	}
	return parts
}

func (c *Cluster) apply(cmd *command, wait bool) error {
	if c.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	af := c.raft.Apply(b, raftTimeout)
	err = af.Error()
	if err != nil || !wait {
		return err
	}

	// Waits until the command has been applied to the FSM of all nodes.
	f := c.raft.Barrier(raftTimeout)
	return f.Error()
}

func (c *Cluster) AddGroup(groupID int, servers ...Server) error {
	return c.apply(
		&command{
			Op:      "add_group",
			GroupID: groupID,
			Servers: servers,
		},
		false,
	)
}

func (c *Cluster) DelGroup(groupID int) error {
	return c.apply(
		&command{
			Op:      "del_group",
			GroupID: groupID,
		},
		false,
	)
}

func (c *Cluster) validateSlotID(slotID int) error {
	if slotID < 0 && slotID >= SlotNum {
		return fmt.Errorf("slot id %d is not in [0, %d)", slotID, SlotNum)
	}
	return nil
}

func (c *Cluster) AssignSlots(toGroupID, startSlotID, stopSlotID int) error {
	if err := c.validateSlotID(startSlotID); err != nil {
		return err
	}

	if err := c.validateSlotID(stopSlotID); err != nil {
		return err
	}

	return c.apply(
		&command{
			Op:          "assign_slots",
			GroupID:     toGroupID,
			StartSlotID: startSlotID,
			StopSlotID:  stopSlotID,
		},
		false,
	)
}

func (c *Cluster) MigrateSlots(toGroupID, startSlotID, stopSlotID int) (err error) {
	if err := c.validateSlotID(startSlotID); err != nil {
		return err
	}

	if err := c.validateSlotID(stopSlotID); err != nil {
		return err
	}

	for slotID := startSlotID; slotID <= stopSlotID; slotID++ {
		slot := c.slots[slotID]

		switch slot.State() {
		case SlotStateOffline:
			return fmt.Errorf("slot %d is offline", slotID)
		case SlotStateInMigration:
			return fmt.Errorf("slot %d is in migration", slotID)
		case SlotStateOnline:
			if slot.Group().ID() == toGroupID {
				// Nothing happens if the slot already belongs to toGroupID.
				continue
			}

			// Change the slot state to pre-migration, and blocks until
			// this operation has been applied to the FSM.
			err = c.apply(
				&command{
					Op:          "change_slot_state",
					GroupID:     toGroupID,
					StartSlotID: slotID,
					SlotState:   SlotStatePreMigration,
				},
				true,
			)
			if err != nil {
				return err
			}
			// Now the slot state (within each node) is pre-migration, and
			// blocks until this operation has been applied to the FSM.
			err = c.apply(
				&command{
					Op:          "change_slot_state",
					GroupID:     toGroupID,
					StartSlotID: slotID,
					SlotState:   SlotStateInMigration,
				},
				true,
			)
			if err != nil {
				return err
			}

			from := slot.FromGroup()
			to := slot.Group()
			from.MigrateSlot(to, slotID)

			// Now the slot has been migrated, change the slot state to online,
			// and blocks until this operation has been applied to the FSM.
			err = c.apply(
				&command{
					Op:          "change_slot_state",
					GroupID:     toGroupID,
					StartSlotID: slotID,
					SlotState:   SlotStateOnline,
				},
				true,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Cluster) getSlotID(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)) % SlotNum)
}

// GetGroupByKey finds the group which manages the slot the key belongs to.
func (c *Cluster) GetGroupByKey(key string) (Group, error) {
	slotID := c.getSlotID(key)
	slot := c.slots[slotID]

	g, from, err := slot.GetWorkingGroups()
	if err != nil {
		return nil, err
	}

	// If the key belongs to a slot which is in migration, always
	// trigger a key migration first to ensure that the key has been
	// migrated from the group from to the group g.
	if from != nil {
		if err := from.MigrateKeys(g, slotID, key); err != nil {
			return nil, err
		}
	}

	return g, nil
}
