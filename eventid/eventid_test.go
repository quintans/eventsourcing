package eventid

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSerialise(t *testing.T) {
	ts := Time(0x0000f00000000000)
	id, _ := uuid.Parse("80e7a863-9aaf-4cb2-b9c4-fc32bcc75d3c")
	eid1 := New(ts, id, 1)
	assert.Equal(t, "Y0000000020EFA33KAQMSCNSRKY35F67BMY0000004", eid1.String())
	eid2 := New(ts, id, 2)
	assert.Equal(t, "Y0000000020EFA33KAQMSCNSRKY35F67BMY0000008", eid2.String())
	assert.Greater(t, eid2.String(), eid1.String())
	ts = Time(0x0000ff0000000000)
	eid3 := New(ts, id, 1)
	assert.Equal(t, "ZW000000020EFA33KAQMSCNSRKY35F67BMY0000004", eid3.String())
	assert.Greater(t, eid3.String(), eid2.String())
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
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.binary()
			s := Marshal(b)
			assert.Equal(t, tt.expected, s)
		})
	}

}
