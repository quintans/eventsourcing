//go:build unit

package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

func TestMarshall(t *testing.T) {
	tc := []struct {
		name     string
		binary   func() []byte
		expected string
	}{
		{
			name: "one",
			binary: func() []byte {
				return []byte{1}
			},
			expected: "04",
		},
		{
			name: "two",
			binary: func() []byte {
				return []byte{2}
			},
			expected: "08",
		},
		{
			name: "three",
			binary: func() []byte {
				return []byte{3}
			},
			expected: "0C",
		},
		{
			name: "empty",
			binary: func() []byte {
				return make([]byte, 3)
			},
			expected: "00000",
		},
		{
			name: "one right",
			binary: func() []byte {
				b := make([]byte, 26)
				b[25] = 0x01
				return b
			},
			expected: "000000000000000000000000000000000000000004",
		},
		{
			name: "just one left",
			binary: func() []byte {
				return []byte{0x80}
			},
			expected: "G0",
		},
		{
			name: "one left",
			binary: func() []byte {
				return []byte{0x80, 0}
			},
			expected: "G000",
		},
		{
			name: "on both ends",
			binary: func() []byte {
				return []byte{0x80, 0, 0x1}
			},
			expected: "G0002",
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.binary()
			s := MarshalBase32(b)
			assert.Equal(t, tt.expected, s)
		})
	}
}

func TestUnmarshall(t *testing.T) {
	tc := []struct {
		name     string
		encoded  string
		expected []byte
	}{
		{
			name:     "decode empty",
			encoded:  "00000",
			expected: []byte{0, 0, 0},
		},
		{
			name:     "decode one right",
			encoded:  "000000000000000000000000000000000000000004",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			name:     "decode just one left",
			encoded:  "G0",
			expected: []byte{0x80},
		},
		{
			name:     "decode one left",
			encoded:  "G000",
			expected: []byte{0x80, 0},
		},
		{
			name:     "decode on both ends",
			encoded:  "G0002",
			expected: []byte{0x80, 0, 0x1},
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			s, err := UnmarshalBase32(tt.encoded)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expected, s)
		})
	}
}
