package util

import (
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

// Hash returns the hash code for s
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func In[T comparable](test T, values ...T) bool {
	for _, v := range values {
		if v == test {
			return true
		}
	}
	return false
}

func MustNewULID() ulid.ULID {
	id, err := NewULID()
	if err != nil {
		panic(err)
	}
	return id
}

func NewULID() (ulid.ULID, error) {
	t := time.Now().UTC()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)

	return ulid.New(ulid.Timestamp(t), entropy)
}
