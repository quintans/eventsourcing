package util

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
