package cluster

type Migrator interface {
	MigrateKeys(to Group, slotID int, keys ...string) error
	MigrateSlot(to Group, slotID int) error
}

type Server string

type Group interface {
	Migrator

	ID() int
	Servers() []Server
}

// NewGroup represent a group constructor that only accepts
// id and servers as arguments.
type NewGroup func(id int, servers []Server) Group
