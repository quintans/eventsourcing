package encoding

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/quintans/faults"
)

// JSON is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
// This can interchangeably use []byte or map[string]interface{}
type JSON struct {
	b []byte
	m map[string]interface{}
}

func JSONOfBytes(b []byte) *JSON {
	if len(b) == 0 {
		return &JSON{}
	}
	return &JSON{b: b}
}

func JSONOfString(s string) *JSON {
	if s == "" {
		return &JSON{}
	}
	return &JSON{b: []byte(s)}
}

func JSONOfMap(m map[string]interface{}) *JSON {
	if len(m) == 0 {
		return &JSON{}
	}
	return &JSON{m: m}
}

func (j JSON) IsZero() bool {
	return len(j.b) == 0 && len(j.m) == 0
}

// MarshalJSON returns m as the JSON encoding of m.
func (j *JSON) MarshalJSON() ([]byte, error) {
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
func (j *JSON) UnmarshalJSON(data []byte) error {
	if j == nil {
		return faults.New("encoding.Json: UnmarshalJSON on nil pointer")
	}
	j.b = data
	return nil
}

var (
	_ json.Marshaler   = (*JSON)(nil)
	_ json.Unmarshaler = (*JSON)(nil)
)

func (j *JSON) Scan(value interface{}) error {
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

func (j *JSON) Value() (driver.Value, error) {
	b, err := j.AsBytes()
	if err != nil || len(b) == 0 {
		return nil, faults.Wrap(err)
	}
	return string(b), nil
}

func (j JSON) String() string {
	if j.IsZero() {
		return "null"
	}
	if len(j.b) != 0 {
		return string(j.b)
	}
	return fmt.Sprintf("%+v", j.m)
}

var _ fmt.Stringer = (*JSON)(nil)

func (j *JSON) AsMap() (map[string]interface{}, error) {
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

func (j *JSON) AsBytes() ([]byte, error) {
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
