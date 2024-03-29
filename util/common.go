package util

import "hash/fnv"

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

func NormalizePartitions(p []int32) []uint32 {
	out := make([]uint32, len(p))
	for k, v := range p {
		out[k] = uint32(v + 1)
	}
	return out
}

func MapMerge[K comparable, V any](ms ...map[K]V) map[K]V {
	out := map[K]V{}
	for _, m := range ms {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// Hash returns the hash code for s
func HashToInt(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	v := h.Sum32()
	return Int32ring(v)
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
