package encoding

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/quintans/faults"
)

// Base64 is a []byte base64 encoded value.
type Base64 []byte

// MarshalJSON returns m as a base64 encoding of m.
func (m Base64) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte{}, nil
	}
	encoded := `"` + base64.StdEncoding.EncodeToString(m) + `"`
	return []byte(encoded), nil
}

// UnmarshalJSON sets *m to a decoded base64.
func (m *Base64) UnmarshalJSON(data []byte) error {
	if m == nil {
		return faults.New("util.Base64: UnmarshalJSON on nil pointer")
	}
	// strip quotes
	data = data[1 : len(data)-1]

	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return faults.Errorf("util.Base64: decode error: %w", err)
	}

	*m = append((*m)[0:0], decoded...)
	return nil
}

var (
	_ json.Marshaler   = (*Base64)(nil)
	_ json.Unmarshaler = (*Base64)(nil)
)

func (m Base64) String() string {
	if m == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(m)
}

func (m Base64) AsString() string {
	if m == nil {
		return ""
	}
	return string(m)
}

func ParseBase64(data string) (Base64, error) {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, faults.Errorf("util.Base64: decode error: %w", err)
	}
	return Base64(decoded), nil
}

var _ fmt.Stringer = (*Base64)(nil)
