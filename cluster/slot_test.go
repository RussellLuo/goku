package cluster_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/RussellLuo/goku/cluster"
)

func TestSlot_MarkOffline(t *testing.T) {
	slotID := 0
	group1 := newGroup(1, []cluster.Server{"server1"})
	group2 := newGroup(2, []cluster.Server{"server2"})

	type wantType struct {
		err       error
		state     cluster.SlotState
		group     cluster.Group
		fromGroup cluster.Group
	}

	cases := []struct {
		in   *cluster.Slot
		want wantType
	}{
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOffline, nil, nil),
			want: wantType{
				err:       fmt.Errorf("cannot change offline slot to offline"),
				state:     cluster.SlotStateOffline,
				group:     nil,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOnline, group1, nil),
			want: wantType{
				err:       nil,
				state:     cluster.SlotStateOffline,
				group:     nil,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStatePreMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change pre-migration slot to offline"),
				state:     cluster.SlotStatePreMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateInMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change in-migration slot to offline"),
				state:     cluster.SlotStateInMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
	}

	for i, c := range cases {
		s := c.in
		err := s.MarkOffline()
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if s.State() != c.want.state {
			t.Errorf("[case %d] state: got(%+v) != want(%+v)", i, s.State(), c.want.state)
		}
		if s.Group() != c.want.group {
			t.Errorf("[case %d] group: got(%+v) != want(%+v)", i, s.Group(), c.want.group)
		}
		if s.FromGroup() != c.want.fromGroup {
			t.Errorf("[case %d] fromGroup: got(%+v) != want(%+v)", i, s.FromGroup(), c.want.fromGroup)
		}
	}
}

func TestSlot_MarkOnline(t *testing.T) {
	slotID := 0
	group1 := newGroup(1, []cluster.Server{"server1"})
	group2 := newGroup(2, []cluster.Server{"server2"})

	type wantType struct {
		err       error
		state     cluster.SlotState
		group     cluster.Group
		fromGroup cluster.Group
	}

	cases := []struct {
		in   *cluster.Slot
		want wantType
	}{
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOffline, nil, nil),
			want: wantType{
				err:       nil,
				state:     cluster.SlotStateOnline,
				group:     group2,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOnline, group1, nil),
			want: wantType{
				err:       fmt.Errorf("cannot change online slot to online"),
				state:     cluster.SlotStateOnline,
				group:     group1,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStatePreMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change pre-migration slot to online"),
				state:     cluster.SlotStatePreMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateInMigration, group2, group1),
			want: wantType{
				err:       nil,
				state:     cluster.SlotStateOnline,
				group:     group2,
				fromGroup: nil,
			},
		},
	}

	for i, c := range cases {
		s := c.in
		err := s.MarkOnline(group2)
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if s.State() != c.want.state {
			t.Errorf("[case %d] state: got(%+v) != want(%+v)", i, s.State(), c.want.state)
		}
		if s.Group() != c.want.group {
			t.Errorf("[case %d] group: got(%+v) != want(%+v)", i, s.Group(), c.want.group)
		}
		if s.FromGroup() != c.want.fromGroup {
			t.Errorf("[case %d] fromGroup: got(%+v) != want(%+v)", i, s.FromGroup(), c.want.fromGroup)
		}
	}
}

func TestSlot_MarkPreMigration(t *testing.T) {
	slotID := 0
	group1 := newGroup(1, []cluster.Server{"server1"})
	group2 := newGroup(2, []cluster.Server{"server2"})

	type wantType struct {
		err       error
		state     cluster.SlotState
		group     cluster.Group
		fromGroup cluster.Group
	}

	cases := []struct {
		in   *cluster.Slot
		want wantType
	}{
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOffline, nil, nil),
			want: wantType{
				err:       fmt.Errorf("cannot change offline slot to pre-migration"),
				state:     cluster.SlotStateOffline,
				group:     nil,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOnline, group1, nil),
			want: wantType{
				err:       nil,
				state:     cluster.SlotStatePreMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStatePreMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change pre-migration slot to pre-migration"),
				state:     cluster.SlotStatePreMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateInMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change in-migration slot to pre-migration"),
				state:     cluster.SlotStateInMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
	}

	for i, c := range cases {
		s := c.in
		err := s.MarkPreMigration(group2)
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if s.State() != c.want.state {
			t.Errorf("[case %d] state: got(%+v) != want(%+v)", i, s.State(), c.want.state)
		}
		if s.Group() != c.want.group {
			t.Errorf("[case %d] group: got(%+v) != want(%+v)", i, s.Group(), c.want.group)
		}
		if s.FromGroup() != c.want.fromGroup {
			t.Errorf("[case %d] fromGroup: got(%+v) != want(%+v)", i, s.FromGroup(), c.want.fromGroup)
		}
	}
}

func TestSlot_MarkInMigration(t *testing.T) {
	slotID := 0
	group1 := newGroup(1, []cluster.Server{"server1"})
	group2 := newGroup(2, []cluster.Server{"server2"})

	type wantType struct {
		err       error
		state     cluster.SlotState
		group     cluster.Group
		fromGroup cluster.Group
	}

	cases := []struct {
		in   *cluster.Slot
		want wantType
	}{
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOffline, nil, nil),
			want: wantType{
				err:       fmt.Errorf("cannot change offline slot to in-migration"),
				state:     cluster.SlotStateOffline,
				group:     nil,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOnline, group1, nil),
			want: wantType{
				err:       fmt.Errorf("cannot change online slot to in-migration"),
				state:     cluster.SlotStateOnline,
				group:     group1,
				fromGroup: nil,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStatePreMigration, group2, group1),
			want: wantType{
				err:       nil,
				state:     cluster.SlotStateInMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateInMigration, group2, group1),
			want: wantType{
				err:       fmt.Errorf("cannot change in-migration slot to in-migration"),
				state:     cluster.SlotStateInMigration,
				group:     group2,
				fromGroup: group1,
			},
		},
	}

	for i, c := range cases {
		s := c.in
		err := s.MarkInMigration()
		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if s.State() != c.want.state {
			t.Errorf("[case %d] state: got(%+v) != want(%+v)", i, s.State(), c.want.state)
		}
		if s.Group() != c.want.group {
			t.Errorf("[case %d] group: got(%+v) != want(%+v)", i, s.Group(), c.want.group)
		}
		if s.FromGroup() != c.want.fromGroup {
			t.Errorf("[case %d] fromGroup: got(%+v) != want(%+v)", i, s.FromGroup(), c.want.fromGroup)
		}
	}
}

func TestSlot_GetWorkingGroups(t *testing.T) {
	slotID := 0
	group1 := newGroup(1, []cluster.Server{"server1"})
	group2 := newGroup(2, []cluster.Server{"server2"})

	type wantType struct {
		err       error
		group     cluster.Group
		fromGroup cluster.Group
		blocked   bool
	}

	cases := []struct {
		in   *cluster.Slot
		want wantType
	}{
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOffline, nil, nil),
			want: wantType{
				err:       fmt.Errorf("slot is offline"),
				group:     nil,
				fromGroup: nil,
				blocked:   false,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateOnline, group1, nil),
			want: wantType{
				err:       nil,
				group:     group1,
				fromGroup: nil,
				blocked:   false,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStatePreMigration, group2, group1),
			want: wantType{
				err:       nil,
				group:     group2,
				fromGroup: group1,
				blocked:   true,
			},
		},
		{
			in: cluster.NewSlot(slotID, cluster.SlotStateInMigration, group2, group1),
			want: wantType{
				err:       nil,
				group:     group2,
				fromGroup: group1,
				blocked:   false,
			},
		},
	}

	for i, c := range cases {
		s := c.in

		go func() {
			// Sleep for 2ms
			time.Sleep(2 * time.Millisecond)
			s.MarkInMigration()
		}()

		start := time.Now()
		g, from, err := s.GetWorkingGroups()
		stop := time.Now()
		// GetWorkingGroups is considered to be blocked, if it consumes
		// greater than 1ms, which is lower than the above sleeping time but
		// obviously much greater than normal non-blocking execution time.
		blocked := stop.Sub(start) > 1*time.Millisecond

		if !reflect.DeepEqual(err, c.want.err) {
			t.Errorf("[case %d] err: got(%+v) != want(%+v)", i, err, c.want.err)
		}
		if g != c.want.group {
			t.Errorf("[case %d] group: got(%+v) != want(%+v)", i, g, c.want.group)
		}
		if from != c.want.fromGroup {
			t.Errorf("[case %d] fromGroup: got(%+v) != want(%+v)", i, from, c.want.fromGroup)
		}
		if blocked != c.want.blocked {
			t.Errorf("[case %d] blocked: got(%+v) != want(%+v)", i, blocked, c.want.blocked)
		}
	}
}
