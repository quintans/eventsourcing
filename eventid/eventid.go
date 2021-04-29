package eventid

import (
	"bytes"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/faults"
)

const (
	EncodingSize      = 25
	EncodedStringSize = 40

	TimestampSize = 6
	UuidSize      = 16
	VersionSize   = 3
)

var ErrInvalidStringSize = errors.New("String size should be 40")

type EventID [EncodingSize]byte

func New(instant time.Time, aggregateID uuid.UUID, version uint32) EventID {
	var eid EventID

	eid.SetTime(instant)
	eid.SetAggregateID(aggregateID)
	eid.SetVersion(version)

	return eid
}

func (e EventID) String() string {
	return encoding.Marshal(e[:])
}

func Parse(encoded string) (EventID, error) {
	if len(encoded) != EncodedStringSize {
		return EventID{}, faults.Errorf("%w: %s", ErrInvalidStringSize, encoded)
	}
	a, err := encoding.Unmarshal(encoded)
	if err != nil {
		return EventID{}, err
	}

	// aggregate uuid - check if is parsable
	_, err = uuid.FromBytes(a[TimestampSize : TimestampSize+UuidSize])
	if err != nil {
		return EventID{}, err
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

func (e *EventID) SetTime(instant time.Time) {
	ts := Timestamp(instant)
	bts := encoding.I64tob(ts) // 8 bytes
	// using only 6 bytes will give us 12293 years
	copy(e[:], bts[2:])
}

func (e EventID) AggregateID() uuid.UUID {
	// ignoring error because it was already successfully parsed
	id, _ := uuid.FromBytes(e[TimestampSize : TimestampSize+UuidSize])

	return id
}

func (e *EventID) SetAggregateID(aggregateID uuid.UUID) {
	bid, _ := aggregateID.MarshalBinary() // 16 bytes
	copy(e[TimestampSize:], bid)
}

func (e EventID) Version() uint32 {
	b := make([]byte, 4)
	copy(b[1:], e[TimestampSize+UuidSize:])
	return encoding.Btoi32(b)
}

func (e *EventID) SetVersion(version uint32) {
	bver := encoding.I32tob(version)           // 4 bytes
	copy(e[TimestampSize+UuidSize:], bver[1:]) // 3bytes
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if id==other, -1 if id < other, and +1 if id > other.
func (e EventID) Compare(other EventID) int {
	return bytes.Compare(e[:], other[:])
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

func DelayEventID(eventID string, offset time.Duration) (string, error) {
	if eventID == "" {
		return eventID, nil
	}

	id, err := Parse(eventID)
	if err != nil {
		return "", err
	}
	t := id.Time()
	// add a safety margin (offset).
	t = t.Add(-offset)
	id.SetTime(t)

	return id.String(), nil
}

func NewEventID(createdAt time.Time, aggregateID string, version uint32) string {
	var id uuid.UUID
	if aggregateID != "" {
		id, _ = uuid.Parse(aggregateID)
	} else {
		id = uuid.UUID{}
	}
	eid := New(createdAt, id, version)
	return eid.String()
}
