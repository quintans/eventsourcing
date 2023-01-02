package eventid

import (
	"database/sql/driver"
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/quintans/faults"

	"github.com/oklog/ulid/v2"
)

const (
	encodedStringSize = 26
)

var (
	ErrInvalidStringSize = errors.New("string size should be 26")
	Zero                 EventID
)

type EventID struct {
	u ulid.ULID
}

type Entropy struct {
	entropy *ulid.MonotonicEntropy
}

func NewEntropy() *Entropy {
	t := time.Now()
	return &Entropy{
		// beaware that entropy isn't safe for concurrent use.
		entropy: ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0),
	}
}

func (e *Entropy) NewID(t time.Time) (EventID, error) {
	return New(t, e.entropy)
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

func MustNew(t time.Time, entropy io.Reader) EventID {
	id, err := New(t, entropy)
	if err != nil {
		panic(err)
	}
	return id
}

func TimeOnly(t time.Time) EventID {
	var id ulid.ULID
	_ = id.SetTime(ulid.Timestamp(t))
	return EventID{u: id}
}

func (e EventID) String() string {
	if e == Zero {
		return ""
	}

	return e.u.String()
}

func (e EventID) IsZero() bool {
	return e == Zero
}

func Parse(encoded string) (EventID, error) {
	if encoded == "" {
		return Zero, nil
	}

	if len(encoded) != encodedStringSize {
		return EventID{}, faults.Errorf("unable to parse event ID '%s'[size=%d]: %w", encoded, len(encoded), ErrInvalidStringSize)
	}
	u, err := ulid.Parse(encoded)
	if err != nil {
		return EventID{}, err
	}

	return EventID{u}, nil
}

func (e EventID) Time() time.Time {
	return ulid.Time(e.u.Time())
}

func (e EventID) OffsetTime(offset time.Duration) EventID {
	ut := e.u.Time()
	t := ulid.Time(ut)
	t = t.Add(offset)
	other := ulid.ULID{}
	_ = other.SetTime(ulid.Timestamp(t))
	_ = other.SetEntropy(e.u.Entropy())
	return EventID{other}
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if e==other, -1 if e < other, and +1 if e > other.
func (e EventID) Compare(other EventID) int {
	return e.u.Compare(other.u)
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
