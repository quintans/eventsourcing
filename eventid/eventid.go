package eventid

import (
	"database/sql/driver"
	"errors"
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

func entropyFactory() *ulid.MonotonicEntropy {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	return ulid.Monotonic(entropy, 0)
}

func New() EventID {
	return EventID{ulid.Make()}
}

func NewWithTime(t time.Time) EventID {
	entropy := entropyFactory()
	id, err := ulid.New(ulid.Timestamp(t), entropy)
	if err != nil {
		panic(err)
	}
	return EventID{id}
}

func NewAfterTime(t time.Time) EventID {
	t = afterPlus1ms(t)

	return NewWithTime(t)
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

type Generator struct {
	entropy *ulid.MonotonicEntropy
	after   time.Time
}

func NewGeneratorNow() *Generator {
	now := time.Now()
	entropy := rand.New(rand.NewSource(now.UnixNano()))
	return &Generator{
		// beware that entropy isn't safe for concurrent use.
		entropy: ulid.Monotonic(entropy, 0),
		after:   now,
	}
}

// NewGenerator generates EventIDs with time.Now().
// If due to clock skews, time.Now() <= t + 1ms, then the time used will be t + 1ms.
func NewGenerator(t time.Time) *Generator {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Generator{
		// beware that entropy isn't safe for concurrent use.
		entropy: ulid.Monotonic(entropy, 0),
		after:   t.Add(time.Millisecond),
	}
}

func (s *Generator) NewID() EventID {
	t := time.Now()
	if t.Before(s.after) {
		t = s.after
	}
	id, err := ulid.New(ulid.Timestamp(t), s.entropy)
	if err != nil {
		panic(err)
	}
	return EventID{id}
}

func afterPlus1ms(last time.Time) time.Time {
	now := time.Now()
	// due to clock skews, 't' can be less or equal than the last update
	// so we make sure that it will be at least 1ms after.
	if now.UnixMilli() <= last.UnixMilli() {
		now = last.Add(time.Millisecond)
	}
	// we only need millisecond precision
	now = now.Truncate(time.Millisecond)
	return now
}
