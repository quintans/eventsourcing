package encoding

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// Base64 is a []byte base64 encoded value.
type Base64 []byte

// MarshalJSON returns m as a base64 encoding of m.
func (m Base64) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte{}, nil
	}
	encoded := base64.StdEncoding.EncodeToString(m)
	return []byte(encoded), nil
}

// UnmarshalJSON sets *m to a decoded base64.
func (m *Base64) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("common.Base64: UnmarshalJSON on nil pointer")
	}
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("common.Base64: decode error: %w", err)
	}

	*m = append((*m)[0:0], decoded...)
	return nil
}

var _ json.Marshaler = (*Base64)(nil)
var _ json.Unmarshaler = (*Base64)(nil)

func (m Base64) String() string {
	if m == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(m)
}

var _ fmt.Stringer = (*Base64)(nil)
