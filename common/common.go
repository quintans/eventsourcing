package common

import (
	"hash/fnv"
	"reflect"
)

// Dereference returns the underlying struct dereference
func Dereference(i interface{}) interface{} {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return i
	}
	v = v.Elem()
	return v.Interface()
}

// Hash returns the hash code for s
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
