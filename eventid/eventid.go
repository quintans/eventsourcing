package eventid

import (
	"database/sql/driver"
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/quintans/faults"

	"github.com/oklog/ulid/v2"

	"github.com/quintans/eventsourcing/encoding"
)

const (
	encodedStringSize   = 26
	encodedStringSizeV2 = 28
)

var (
	ErrInvalidStringSize = errors.New("string size should be 26 or 28")
	Zero                 EventID
)

type EventID struct {
	u     ulid.ULID
	count uint8
}

func EntropyFactory(t time.Time) *ulid.MonotonicEntropy {
	return ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
}

func New(t time.Time, entropy io.Reader) (EventID, error) {
	id, err := ulid.New(ulid.Timestamp(t), entropy)
	if err != nil {
		return Zero, err
	}
	return EventID{u: id}, nil
}

func TimeOnly(t time.Time) EventID {
	var id ulid.ULID
	id.SetTime(ulid.Timestamp(t))
	return EventID{u: id}
}

func (e EventID) String() string {
	if e == Zero {
		return ""
	}

	s := e.u.String()
	if e.count == 0 {
		return s
	}
	return s + encoding.MarshalBase32([]byte{e.count})
}

func (e EventID) IsZero() bool {
	return e == Zero
}

func Parse(encoded string) (EventID, error) {
	if encoded == "" {
		return Zero, nil
	}

	if len(encoded) != encodedStringSize && len(encoded) != encodedStringSizeV2 {
		return EventID{}, faults.Errorf("unable to parse event ID '%s': %w", encoded, ErrInvalidStringSize)
	}

	var count uint8
	if len(encoded) == encodedStringSizeV2 {
		enc := encoded[encodedStringSize:]
		a, err := encoding.UnmarshalBase32(enc)
		if err != nil {
			return EventID{}, err
		}
		count = a[0]
		encoded = encoded[:encodedStringSize]
	}
	u, err := ulid.Parse(encoded)
	if err != nil {
		return EventID{}, err
	}

	return EventID{u, count}, nil
}

func (e EventID) SetCount(c uint8) EventID {
	return EventID{e.u, c}
}

func (e EventID) Count() uint8 {
	return e.count
}

func (e EventID) OffsetTime(offset time.Duration) EventID {
	ut := e.u.Time()
	t := ulid.Time(ut)
	t = t.Add(offset)
	other := ulid.ULID{}
	other.SetTime(ulid.Timestamp(t))
	other.SetEntropy(e.u.Entropy())
	return EventID{other, e.count}
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if e==other, -1 if e < other, and +1 if e > other.
func (e EventID) Compare(other EventID) int {
	c := e.u.Compare(other.u)
	if c != 0 {
		return c
	}

	if e.count == other.count {
		return 0
	} else if e.count < other.count {
		return -1
	} else {
		return 1
	}
}

// MarshalJSON returns m as string.
func (e EventID) MarshalJSON() ([]byte, error) {
	encoded := `"` + e.String() + `"`
	return []byte(encoded), nil
}

// UnmarshalJSON sets *m to a decoded base64.
func (e *EventID) UnmarshalJSON(data []byte) error {
	if e == nil {
		return faults.New("eventid.UnmarshalJSON: UnmarshalJSON on nil pointer")
	}
	// strip quotes
	data = data[1 : len(data)-1]

	decoded, err := Parse(string(data))
	if err != nil {
		return faults.Errorf("decode error: %w", err)
	}

	*e = decoded
	return nil
}

func (e *EventID) Scan(value interface{}) error {
	if value == nil {
		*e = Zero
		return nil
	}

	switch s := value.(type) {
	case string:
		eID, err := Parse(s)
		if err != nil {
			return err
		}
		*e = eID
	case []byte:
		eID, err := Parse(string(s))
		if err != nil {
			return err
		}
		*e = eID
	}
	return nil
}

func (e EventID) Value() (driver.Value, error) {
	return e.String(), nil
}
