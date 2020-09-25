package eventid

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	EncodingSize      = 26
	EncodedStringSize = 42
	ByteSize          = 8
	TimestampSize     = 6
	UuidSize          = 16
	VersionSize       = 4
	// Encoding follows Croford's Base 32 https://www.crockford.com/base32.html
	Encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	bitShift = 5
)

var (
	ErrInvalidString     = errors.New("String contains invalid characters")
	ErrInvalidStringSize = errors.New("String size should be 42")
)

type EventID [EncodingSize]byte

func New(instant time.Time, aggregateID uuid.UUID, version uint32) EventID {
	var eid EventID

	eid.SetTime(instant)
	eid.SetAggregateID(aggregateID)
	eid.SetVersion(version)

	return eid
}

func (e EventID) String() string {
	return Marshal(e[:])
}

func Parse(encoded string) (EventID, error) {
	if len(encoded) != EncodedStringSize {
		return EventID{}, ErrInvalidStringSize
	}
	a, err := Unmarshal(encoded)
	if err != nil {
		return EventID{}, err
	}

	// aggregate uuid - check if is parsable
	_, err = uuid.ParseBytes(a[TimestampSize : TimestampSize+UuidSize])
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
	ts := btoi64(b)
	return Time(ts)
}

func (e *EventID) SetTime(instant time.Time) {
	ts := Timestamp(instant)
	bts := i64tob(ts) // 8 bytes
	// using only 6 bytes will give us 12293 years
	copy(e[:], bts[2:])
}

func (e EventID) AggregateID() uuid.UUID {
	id, _ := uuid.ParseBytes(e[TimestampSize : TimestampSize+UuidSize])

	return id
}

func (e *EventID) SetAggregateID(aggregateID uuid.UUID) {
	bid, _ := aggregateID.MarshalBinary() // 16 bytes
	copy(e[TimestampSize:], bid)
}

func (e EventID) Version() uint32 {
	return btoi32(e[TimestampSize+UuidSize:])
}

func (e *EventID) SetVersion(version uint32) {
	bver := i32tob(version) // 4 bytes
	copy(e[TimestampSize+UuidSize:], bver)
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

func i64tob(val uint64) []byte {
	r := make([]byte, 8)
	binary.BigEndian.PutUint64(r, val)
	return r
}

func btoi64(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	binary.BigEndian.PutUint32(r, val)
	return r
}

func btoi32(val []byte) uint32 {
	return binary.BigEndian.Uint32(val)
}

func Marshal(a []byte) string {
	encSize := (len(a)*ByteSize)/5 + 1
	result := make([]byte, encSize)
	shift2 := (ByteSize - bitShift)
	var mask2 byte = 0xff << shift2
	d := a
	for i := 0; i < encSize; i++ {
		c := d[0] & mask2
		c = c >> shift2
		result[i] = Encoding[c]
		d = shiftBytesLeft(d, bitShift)
	}
	return string(result)
}

func Unmarshal(encoded string) ([]byte, error) {
	decSize := ((len(encoded) - 1) * 5) / ByteSize
	result := make([]byte, decSize)
	for i := decSize - 1; i >= 0; i-- {
		idx := strings.Index(Encoding, string(encoded[i]))
		if idx < 0 {
			return nil, ErrInvalidString
		}
		result[decSize] = result[decSize] | byte(idx)
		result = shiftBytesLeft(result, bitShift)
	}
	return result, nil
}

// shiftBytesLeft shift bytes left by shift amount.
//
// It returns the resulting shifted array
func shiftBytesLeft(a []byte, shift int) (dst []byte) {
	n := len(a)
	dst = make([]byte, n)
	var mask byte = 0xff << shift
	shift2 := (8 - shift)
	// shifting left
	for i := 0; i < n-1; i++ {
		dst[i] = a[i] << shift
		dst[i] = (dst[i] & mask) | (a[i+1] >> shift2)
	}
	dst[n-1] = a[n-1] << shift

	return dst
}
