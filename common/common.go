package common

import (
	"time"
)

// Element represents an element of a set.
type Element struct {
	Member    string
	Timestamp int64
	// The remaining time to live of this member
	//     positive value: means volatile
	//     zero: means persistent
	//     negative value: invalid
	TTL time.Duration
}

type Inserter interface {
	Insert(slotID int, key, member string, timestamp int64, ttl time.Duration) (bool, error)
}

type Deleter interface {
	Delete(slotID int, key, member string, timestamp int64) (bool, error)
}

type Selector interface {
	Select(slotID int, key string, timestamp int64) ([]Element, error)
}

type Scanner interface {
	Keys(slotID, batchSize int) <-chan []string
}
