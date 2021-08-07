package encoding

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/quintans/faults"
)

// Json is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
type Json []byte

// MarshalJSON returns m as the JSON encoding of m.
func (j Json) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	return j, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (j *Json) UnmarshalJSON(data []byte) error {
	if j == nil {
		return faults.New("common.Json: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

var (
	_ json.Marshaler   = (*Json)(nil)
	_ json.Unmarshaler = (*Json)(nil)
)

func (j *Json) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}

	switch s := value.(type) {
	case string:
		*j = []byte(s)
	case []byte:
		*j = append((*j)[0:0], s...)
	}
	return nil
}

func (j Json) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

func (j Json) String() string {
	if j == nil {
		return "null"
	}
	return string(j)
}

var _ fmt.Stringer = (*Json)(nil)

func (j Json) AsMap() (map[string]interface{}, error) {
	m := map[string]interface{}{}
	if len(j) == 0 {
		return m, nil
	}
	err := json.Unmarshal(j, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
