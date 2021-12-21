package eventid

import (
	"math/rand"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func entropyFactory(t time.Time) *ulid.MonotonicEntropy {
	return ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
}

func TestMarshal(t *testing.T) {
	ts := ulid.Time(0x0000f00000000000)
	entropy := entropyFactory(ts)
	eid, err := New(ts, entropy)
	require.NoError(t, err)
	assert.Equal(t, "7G00000000D0AJ8894M178DT3P", eid.String())
}

func TestUnmarshalWithCount(t *testing.T) {
	ts := ulid.Time(0x0000f00000000000)
	entropy := entropyFactory(ts)
	eid, err := New(ts, entropy)
	require.NoError(t, err)

	testCases := []struct {
		name      string
		eventID   string
		eID       EventID
		wantError bool
	}{
		{
			name:    "success",
			eventID: "7G00000000D0AJ8894M178DT3P",
			eID:     eid,
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
			if (err != nil) != tc.wantError {
				t.Fatalf("error presence (%t) and wantError (%t) don't match", err != nil, tc.wantError)
			}
			require.True(t, tc.eID.Compare(eID) == 0)
		})
	}
}
