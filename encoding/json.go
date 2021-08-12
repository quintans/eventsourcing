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
// This can interchangeably use []byte or map[string]interface{}
type Json struct {
	b []byte
	m map[string]interface{}
}

func JsonOfBytes(b []byte) *Json {
	if len(b) == 0 {
		return &Json{}
	}
	return &Json{b: b}
}

func JsonOfString(s string) *Json {
	if len(s) == 0 {
		return &Json{}
	}
	return &Json{b: []byte(s)}
}

func JsonOfMap(m map[string]interface{}) *Json {
	if len(m) == 0 {
		return &Json{}
	}
	return &Json{m: m}
}

func (j Json) IsZero() bool {
	return len(j.b) == 0 && len(j.m) == 0
}

// MarshalJSON returns m as the JSON encoding of m.
func (j *Json) MarshalJSON() ([]byte, error) {
	b, err := j.AsBytes()
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return []byte("null"), nil
	}
	return b, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (j *Json) UnmarshalJSON(data []byte) error {
	if j == nil {
		return faults.New("encoding.Json: UnmarshalJSON on nil pointer")
	}
	j.b = data
	return nil
}

var (
	_ json.Marshaler   = (*Json)(nil)
	_ json.Unmarshaler = (*Json)(nil)
)

func (j *Json) Scan(value interface{}) error {
	if j == nil {
		return faults.New("encoding.Json: Scan on nil pointer")
	}

	if value == nil {
		j.b, j.m = nil, nil
		return nil
	}

	switch s := value.(type) {
	case string:
		j.b = []byte(s)
	case []byte:
		j.b = s
	}
	return nil
}

func (j *Json) Value() (driver.Value, error) {
	b, err := j.AsBytes()
	if err != nil || len(b) == 0 {
		return nil, faults.Wrap(err)
	}
	return string(b), nil
}

func (j Json) String() string {
	if j.IsZero() {
		return "null"
	}
	if len(j.b) != 0 {
		return string(j.b)
	}
	return fmt.Sprintf("%+v", j.m)
}

var _ fmt.Stringer = (*Json)(nil)

func (j *Json) AsMap() (map[string]interface{}, error) {
	if j == nil {
		return nil, nil
	}

	if len(j.m) != 0 {
		return j.m, nil
	}
	if len(j.b) == 0 {
		return nil, nil
	}

	j.m = map[string]interface{}{}
	if err := json.Unmarshal(j.b, &j.m); err != nil {
		return nil, faults.Wrap(err)
	}
	return j.m, nil
}

func (j *Json) AsBytes() ([]byte, error) {
	if j == nil {
		return nil, nil
	}

	if len(j.b) != 0 {
		return j.b, nil
	}
	if len(j.m) == 0 {
		return nil, nil
	}

	var err error
	j.b, err = json.Marshal(j.m)
	return j.b, faults.Wrap(err)
}
