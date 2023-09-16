package ids

import (
	"database/sql/driver"
	"errors"
	"hash/fnv"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"
)

func New() AggID {
	return AggID(ulid.Make())
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

type AggID ulid.ULID

func (id AggID) String() string {
	return ulid.ULID(id).String()
}

func (id *AggID) UnmarshalText(v []byte) error {
	var u ulid.ULID
	err := u.UnmarshalText([]byte(v))
	if err != nil {
		return faults.Wrap(err)
	}

	*id = AggID(u)
	return nil
}

func (id AggID) MarshalText() ([]byte, error) {
	return ulid.ULID(id).MarshalText()
}

func (id AggID) Value() (driver.Value, error) {
	return ulid.ULID(id).String(), nil
}

func (id *AggID) Scan(src interface{}) error {
	switch x := src.(type) {
	case nil:
		return nil
	case string:
		return id.UnmarshalText([]byte(x))
	case []byte:
		return id.UnmarshalText(x)
	}

	return errors.New("AggID: source value must be a string or a byte slice")
}
