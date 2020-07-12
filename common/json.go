package common

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Json is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
type Json []byte

// MarshalJSON returns m as the JSON encoding of m.
func (m Json) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (m *Json) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("eventstore.Json: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

var _ json.Marshaler = (*Json)(nil)
var _ json.Unmarshaler = (*Json)(nil)

func (m Json) String() string {
	if m == nil {
		return "null"
	}
	return string(m)
}

var _ fmt.Stringer = (*Json)(nil)
