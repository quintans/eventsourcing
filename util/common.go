package util

import (
	"crypto/rand"
	"hash/fnv"
	"time"

	"github.com/oklog/ulid/v2"
)

// Hash returns the hash code for s
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func Int32ring(x uint32) int32 {
	h := int32(x)
	// we want a positive value so that partitioning (mod) results in a positive value.
	// if h overflows, becoming negative, setting sign bit to zero will make the overflow start from zero
	if h < 0 {
		// setting sign bit to zero
		h &= 0x7fffffff
	}
	return h
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
	entropy := ulid.Monotonic(rand.Reader, 0)

	return ulid.New(ulid.Timestamp(t), entropy)
}

func IfZero[T comparable](test, def T) T {
	var zero T
	if test == zero {
		return def
	}
	return def
}

func CalcPartition(hash, partitions uint32) uint32 {
	if partitions <= 1 {
		return 1
	}
	return (hash % partitions) + 1
}
