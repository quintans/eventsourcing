package eventid

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalWithCount(t *testing.T) {
	testCases := []struct {
		name    string
		eventID string
		count   uint8
	}{
		{
			name:    "no_count",
			eventID: "7G00000000D0AJ8894M178DT3P",
		},
		{
			name:    "count_1",
			eventID: "7G00000000D0AJ8894M178DT3P04",
			count:   1,
		},
		{
			name:    "count_200",
			eventID: "7G00000000D0AJ8894M178DT3PS0",
			count:   200,
		},
	}

	ts := ulid.Time(0x0000f00000000000)
	entropy := EntropyFactory(ts)
	eid, err := New(ts, entropy)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, err)
			eid = eid.SetCount(tc.count)
			assert.Equal(t, tc.eventID, eid.String())
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
			eventID: "7G00000000D0AJ8894M178DT3P",
		},
		{
			name:    "count_1",
			eventID: "7G00000000D0AJ8894M178DT3P04",
			count:   1,
		},
		{
			name:    "count_200",
			eventID: "7G00000000D0AJ8894M178DT3PS0",
			count:   200,
		},
		{
			name:      "wrong_size",
			eventID:   "Y0000000020EFA33KAQMSCNSRKY35F67BMY000010400",
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eID, err := Parse(tc.eventID)
			if tc.wantError {
				require.Error(t, err)
				return
			}

			require.Nil(t, err, "unable to parse event ID")
			assert.Equal(t, tc.count, eID.Count())
		})
	}
}
