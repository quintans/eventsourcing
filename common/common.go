package common

import (
	"hash/fnv"
	"reflect"

	"github.com/google/uuid"
)

const (
	// MinEventID is the lowest event ID
	MinEventID = ""
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

// Hash returns the hash code for u
func Hash(u uuid.UUID) uint32 {
	h := fnv.New32a()
	h.Write(u[:])
	return h.Sum32()
}

func In(test string, values ...string) bool {
	for _, v := range values {
		if v == test {
			return true
		}
	}
	return false
}
