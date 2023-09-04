package ids

import (
	"hash/fnv"

	"github.com/oklog/ulid/v2"
)

func New() ulid.ULID {
	return ulid.Make()
}

// Hash returns the hash code for s
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func HashInt(s string) int32 {
	return Int32ring(Hash(s))
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
