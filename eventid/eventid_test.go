package eventid

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/quintans/eventstore/base32"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerialise(t *testing.T) {
	first := "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001"
	second := "Y0000000020EFA33KAQMSCNSRKY35F67BMY00002"
	third := "ZW000000020EFA33KAQMSCNSRKY35F67BMY00001"

	ts := Time(0x0000f00000000000)
	id, _ := uuid.Parse("80e7a863-9aaf-4cb2-b9c4-fc32bcc75d3c")
	eid1 := New(ts, id, 1)
	assert.Equal(t, first, eid1.String())

	eid, err := Parse(first)
	require.NoError(t, err)
	fmt.Println("    ts:", ts)
	fmt.Println("eid ts:", eid.Time())
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(1), eid.Version())

	eid2 := New(ts, id, 2)
	assert.Equal(t, second, eid2.String())
	assert.Greater(t, eid2.String(), eid1.String())

	eid, err = Parse(second)
	require.NoError(t, err)
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(2), eid.Version())

	ts = Time(0x0000ff0000000000)
	eid3 := New(ts, id, 1)
	assert.Equal(t, third, eid3.String())
	assert.Greater(t, eid3.String(), eid2.String())

	eid, err = Parse(third)
	require.NoError(t, err)
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(1), eid.Version())
}

func TestMarshall(t *testing.T) {
	tc := []struct {
		name     string
		binary   func() []byte
		expected string
	}{
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
			s := base32.Marshal(b)
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
			s, err := base32.Unmarshal(tt.encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, s)
		})
	}
}
