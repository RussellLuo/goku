package cluster_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/RussellLuo/goku/cluster"
)

// group is a mock instance that implements the Group interface.
type group struct {
	id      int
	servers []cluster.Server
}

func newGroup(id int, servers []cluster.Server) cluster.Group {
	return &group{id: id, servers: servers}
}

func (g *group) ID() int                                                        { return g.id }
func (g *group) Servers() []cluster.Server                                      { return g.servers }
func (g *group) MigrateKeys(to cluster.Group, slotID int, keys ...string) error { return nil }
func (g *group) MigrateSlot(to cluster.Group, slotID int) error                 { return nil }

func newAndOpenClusters(t *testing.T, num int) ([]*cluster.Cluster, func()) {
	if num <= 0 {
		panic("num must be greater than 0")
	}

	tmpDirs := make([]string, num)
	clusters := make([]*cluster.Cluster, num)

	for i := 0; i < num; i++ {
		iStr := strconv.Itoa(i)
		tmpDirs[i], _ = ioutil.TempDir("", "store"+iStr)
		clusters[i] = cluster.NewCluster("test"+iStr, newGroup, "127.0.0.1:1200"+iStr, tmpDirs[i])
		if err := clusters[i].Open(i == 0, "node"+iStr); err != nil {
			t.Fatalf("failed to open cluster: %s", err)
		}
	}

	// Simple way to ensure there is a leader.
	time.Sleep(2 * time.Second)

	for i := 1; i < num; i++ {
		iStr := strconv.Itoa(i)
		if err := clusters[0].Join("node"+iStr, "127.0.0.1:1200"+iStr); err != nil {
			t.Fatalf("failed to join node0: %s", err)
		}
	}

	cleanup := func() {
		for _, c := range clusters {
			c.Close(true)
		}
		for _, t := range tmpDirs {
			os.RemoveAll(t)
		}
	}

	return clusters, cleanup
}

func TestEmptyCluster(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 1)
	defer cleanup()
	c := clusters[0]

	groups := c.Groups()
	if len(groups) != 0 {
		t.Errorf("groups(%v) are not empty", groups)
	}

	slots := c.Slots()
	for _, s := range slots {
		if s.State() != cluster.SlotStateOffline {
			t.Errorf("not all slots(%v) are offline", slots)
		}
	}
}

func TestCluster_AddGroup(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 2)
	defer cleanup()
	c1 := clusters[0]
	c2 := clusters[1]

	c1.AddGroup(1, "server1", "server2")
	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	g1 := c1.Groups()
	if len(g1) == 0 {
		t.Errorf("g1(%v) is not added", g1)
	}

	g2 := c2.Groups()
	if !reflect.DeepEqual(g1, g2) {
		t.Errorf("g1(%v) != g2(%v)", g1, g2)
	}
}

func TestCluster_DelGroup(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 2)
	defer cleanup()
	c1 := clusters[0]
	c2 := clusters[1]

	c1.AddGroup(1, "server1", "server2")
	c1.DelGroup(1)
	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	g1 := c1.Groups()
	if len(g1) != 0 {
		t.Errorf("g1(%v) is not deleted", g1)
	}

	g2 := c2.Groups()
	if !reflect.DeepEqual(g1, g2) {
		t.Errorf("g1(%v) != g2(%v)", g1, g2)
	}
}

func TestCluster_AssignSlots(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 2)
	defer cleanup()
	c1 := clusters[0]
	c2 := clusters[1]

	c1.AddGroup(1, "server1", "server2")
	c1.AssignSlots(1, 0, cluster.SlotNum-1)
	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	validate := func(slots map[int]*cluster.Slot) {
		for _, s := range slots {
			if s.Group().ID() != 1 || s.State() != cluster.SlotStateOnline {
				t.Errorf("slot(%v) is not assigned to group 1", s)
			}
		}
	}
	validate(c1.Slots())
	validate(c2.Slots())
}

func TestCluster_MigrateSlots(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 2)
	defer cleanup()
	c1 := clusters[0]
	c2 := clusters[1]

	c1.AddGroup(1, "server1", "server2")
	c1.AssignSlots(1, 0, cluster.SlotNum-1)
	c1.AddGroup(2, "server3", "server4")
	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	if err := c1.MigrateSlots(2, 0, 10); err != nil {
		t.Error(err)
	}

	validate := func(slots map[int]*cluster.Slot) {
		for i, s := range slots {
			if i >= 0 && i <= 10 {
				if s.Group().ID() != 2 || s.State() != cluster.SlotStateOnline {
					t.Errorf("slot (id:%d, state:%d, groupID:%d) is not assigned to group 2", i, s.State(), s.Group().ID())
				}
			} else {
				if s.Group().ID() != 1 || s.State() != cluster.SlotStateOnline {
					t.Errorf("slot (id:%d, state:%d, groupID:%d) does not belong to group 1", i, s.State(), s.Group().ID())
				}
			}
		}
	}
	validate(c1.Slots())
	validate(c2.Slots())
}

func TestCluster_GetGroupByKey(t *testing.T) {
	clusters, cleanup := newAndOpenClusters(t, 2)
	defer cleanup()
	c1 := clusters[0]
	c2 := clusters[1]

	c1.AddGroup(1, "server1", "server2")
	c1.AddGroup(2, "server3", "server4")
	c1.AssignSlots(1, 0, cluster.SlotNum/2-1)
	c1.AssignSlots(2, cluster.SlotNum/2, cluster.SlotNum-1)
	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	validate := func(c *cluster.Cluster, key string, want int) {
		g, err := c.GetGroupByKey(key)
		if err != nil {
			t.Error(err)
		}
		if g.ID() != want {
			t.Errorf("key '%s' belongs to group %d (want: %d)", key, g.ID(), want)
		}
	}
	validate(c1, "foo", 1)
	validate(c2, "foo", 1)
	validate(c1, "barx", 2)
	validate(c2, "barx", 2)
}
