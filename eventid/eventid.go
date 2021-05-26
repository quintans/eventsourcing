package eventid

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/encoding"
)

const (
	EncodingSize        = 26
	EncodedStringSize   = 40
	EncodedStringSizeV2 = 42

	TimestampSize = 6
	UuidSize      = 16
	VersionSize   = 3
	CountSize     = 1

	versionPos = TimestampSize + UuidSize
	countPos   = TimestampSize + UuidSize + VersionSize
)

var (
	ErrInvalidStringSize = errors.New("string size should be 40 or 42")
	Zero                 EventID
)

type EventID [EncodingSize]byte

func New(instant time.Time, aggregateID uuid.UUID, version uint32) EventID {
	return NewV2(instant, aggregateID, version, 0)
}

func NewV2(instant time.Time, aggregateID uuid.UUID, version uint32, count uint8) EventID {
	var eid EventID

	eid.setTime(instant)
	eid.setAggregateID(aggregateID)
	eid.setVersion(version)
	eid.setCount(count)

	return eid
}

func (e EventID) String() string {
	if e == Zero {
		return ""
	}

	if e[countPos] == 0 {
		return encoding.MarshalBase32(e[:countPos])
	}
	return encoding.MarshalBase32(e[:])
}

func (e EventID) IsZero() bool {
	return e == Zero
}

func Parse(encoded string) (EventID, error) {
	if encoded == "" {
		return Zero, nil
	}

	if len(encoded) != EncodedStringSize && len(encoded) != EncodedStringSizeV2 {
		return EventID{}, faults.Errorf("unable to parse event ID '%s': %w", encoded, ErrInvalidStringSize)
	}
	a, err := encoding.UnmarshalBase32(encoded)
	if err != nil {
		return EventID{}, err
	}

	// aggregate uuid - check if is parsable
	_, err = uuid.FromBytes(a[TimestampSize:versionPos])
	if err != nil {
		return EventID{}, faults.Errorf("unable to parse aggregate ID component from '%s': %w", encoded, err)
	}

	var eid EventID
	copy(eid[:], a)

	return eid, nil
}

func (e EventID) Time() time.Time {
	b := make([]byte, 8)
	copy(b[2:], e[:TimestampSize])
	ts := encoding.Btoi64(b)
	return Time(ts)
}

func (e *EventID) setTime(instant time.Time) {
	ts := Timestamp(instant)
	bts := encoding.I64tob(ts) // 8 bytes
	// using only 6 bytes will give us 12293 years
	copy(e[:], bts[2:])
}

func (e EventID) AggregateID() uuid.UUID {
	// ignoring error because it was already successfully parsed
	id, _ := uuid.FromBytes(e[TimestampSize:versionPos])

	return id
}

func (e *EventID) setAggregateID(aggregateID uuid.UUID) {
	bid, _ := aggregateID.MarshalBinary() // 16 bytes
	copy(e[TimestampSize:], bid)
}

func (e EventID) Version() uint32 {
	b := make([]byte, 4)
	copy(b[1:], e[TimestampSize+UuidSize:])
	return encoding.Btoi32(b)
}

func (e *EventID) setVersion(version uint32) {
	bver := encoding.I32tob(version)           // 4 bytes
	copy(e[TimestampSize+UuidSize:], bver[1:]) // 3 bytes
}

func (e EventID) Count() uint8 {
	return e[countPos]
}

func (e *EventID) setCount(count uint8) {
	e[countPos] = count // 1 byte
}

func (e EventID) WithCount(count uint8) EventID {
	e[countPos] = count // 1 byte
	return e
}

func (e EventID) OffsetTime(offset time.Duration) EventID {
	var other EventID
	copy(other[:], e[:])
	t := e.Time()
	t = t.Add(offset)
	return other
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if e==other, -1 if e < other, and +1 if e > other.
func (e EventID) Compare(other EventID) int {
	return bytes.Compare(e[:], other[:])
}

// MarshalJSON returns m as string.
func (m EventID) MarshalJSON() ([]byte, error) {
	encoded := `"` + m.String() + `"`
	return []byte(encoded), nil
}

// UnmarshalJSON sets *m to a decoded base64.
func (m *EventID) UnmarshalJSON(data []byte) error {
	if m == nil {
		return faults.New("common.Base64: UnmarshalJSON on nil pointer")
	}
	// strip quotes
	data = data[1 : len(data)-1]

	decoded, err := Parse(string(data))
	if err != nil {
		return faults.Errorf("decode error: %w", err)
	}

	*m = decoded
	return nil
}

func (m *EventID) Scan(value interface{}) error {
	if value == nil {
		*m = Zero
		return nil
	}

	switch s := value.(type) {
	case string:
		eID, err := Parse(s)
		if err != nil {
			return err
		}
		*m = eID
	case []byte:
		eID, err := Parse(string(s))
		if err != nil {
			return err
		}
		*m = eID
	}
	return nil
}

func (m EventID) Value() (driver.Value, error) {
	return m.String(), nil
}

// Timestamp converts a time.Time to Unix milliseconds.
func Timestamp(t time.Time) uint64 {
	return uint64(t.Unix())*1000 +
		uint64(t.Nanosecond()/int(time.Millisecond))
}

// Time converts Unix milliseconds in the format
// returned by the Timestamp function to a time.Time.
func Time(ms uint64) time.Time {
	s := int64(ms / 1e3)
	ns := int64((ms % 1e3) * 1e6)
	return time.Unix(s, ns)
}
