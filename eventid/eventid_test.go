package eventid_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing/eventid"
)

func TestMarshal(t *testing.T) {
	first := "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001"
	second := "Y0000000020EFA33KAQMSCNSRKY35F67BMY00002"
	third := "ZW000000020EFA33KAQMSCNSRKY35F67BMY00001"

	ts := eventid.Time(0x0000f00000000000)
	id, _ := uuid.Parse("80e7a863-9aaf-4cb2-b9c4-fc32bcc75d3c")
	eid1 := eventid.New(ts, id, 1)
	assert.Equal(t, first, eid1.String())

	eid, err := eventid.Parse(first)
	require.NoError(t, err)
	fmt.Println("    ts:", ts)
	fmt.Println("eid ts:", eid.Time())
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(1), eid.Version())

	eid2 := eventid.New(ts, id, 2)
	assert.Equal(t, second, eid2.String())
	assert.Greater(t, eid2.String(), eid1.String())

	eid, err = eventid.Parse(second)
	require.NoError(t, err)
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(2), eid.Version())

	ts = eventid.Time(0x0000ff0000000000)
	eid3 := eventid.New(ts, id, 1)
	assert.Equal(t, third, eid3.String())
	assert.Greater(t, eid3.String(), eid2.String())

	eid, err = eventid.Parse(third)
	require.NoError(t, err)
	assert.Equal(t, ts, eid.Time())
	assert.Equal(t, id.String(), eid.AggregateID().String())
	assert.Equal(t, uint32(1), eid.Version())

	s := eventid.New(ts, uuid.UUID{}, 0).String()
	assert.Equal(t, "ZW00000000000000000000000000000000000000", s)
}

func TestMarshalWithCount(t *testing.T) {
	testCases := []struct {
		name    string
		eventID string
		count   uint8
	}{
		{
			name:    "no_count",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001",
		},
		{
			name:    "count_1",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY0000104",
			count:   1,
		},
		{
			name:    "count_200",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001S0",
			count:   200,
		},
	}

	ts := eventid.Time(0x0000f00000000000)
	id, _ := uuid.Parse("80e7a863-9aaf-4cb2-b9c4-fc32bcc75d3c")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eid1 := eventid.NewV2(ts, id, 1, tc.count)
			assert.Equal(t, tc.eventID, eid1.String())
		})
	}
}

func TestUnmarshalWithCount(t *testing.T) {
	testCases := []struct {
		name      string
		eventID   string
		count     uint8
		wantError bool
	}{
		{
			name:    "count_0",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001",
		},
		{
			name:    "count_1",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY0000104",
			count:   1,
		},
		{
			name:    "count_200",
			eventID: "Y0000000020EFA33KAQMSCNSRKY35F67BMY00001S0",
			count:   200,
		},
		{
			name:      "wrong_size",
			eventID:   "Y0000000020EFA33KAQMSCNSRKY35F67BMY000010400",
			wantError: true,
		},
	}

	ts := eventid.Time(0x0000f00000000000)
	id, _ := uuid.Parse("80e7a863-9aaf-4cb2-b9c4-fc32bcc75d3c")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eID, err := eventid.Parse(tc.eventID)
			if tc.wantError {
				require.Error(t, err)
				return
			}

			require.Nil(t, err, "unable to parse event ID")
			require.Equal(t, ts, eID.Time())
			require.Equal(t, id, eID.AggregateID())
			assert.Equal(t, tc.count, eID.Count())
		})
	}
}
