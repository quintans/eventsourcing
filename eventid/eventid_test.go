package eventid

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
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
