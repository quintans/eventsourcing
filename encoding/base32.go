package encoding

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/quintans/faults"
)

const (
	ByteSize = 8
	// Encoding follows Croford's Base 32 https://www.crockford.com/base32.html
	Encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	bitShift = 5
)

var (
	ErrInvalidString = errors.New("String contains invalid characters")
)

func I64tob(val uint64) []byte {
	r := make([]byte, 8)
	binary.BigEndian.PutUint64(r, val)
	return r
}

func Btoi64(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}

func I32tob(val uint32) []byte {
	r := make([]byte, 4)
	binary.BigEndian.PutUint32(r, val)
	return r
}

func Btoi32(val []byte) uint32 {
	return binary.BigEndian.Uint32(val)
}

func I16tob(val uint16) []byte {
	r := make([]byte, 2)
	binary.BigEndian.PutUint16(r, val)
	return r
}

func Btoi16(val []byte) uint16 {
	return binary.BigEndian.Uint16(val)
}

func Marshal(a []byte) string {
	bits := len(a) * ByteSize
	encSize := bits / 5
	if bits%5 != 0 {
		encSize++
	}
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
	decSize := (len(encoded) * 5) / ByteSize
	// added one byte more to be the feeder
	result := make([]byte, decSize+1)
	encodeSize := len(encoded)
	shift2 := (ByteSize - bitShift)
	for i := 0; i < encodeSize; i++ {
		idx := strings.Index(Encoding, string(encoded[i]))
		if idx < 0 {
			return nil, faults.Wrap(ErrInvalidString)
		}
		b := byte(idx) << shift2
		result[decSize] |= b
		// ignore the last iteration
		if i < encodeSize-1 {
			result = shiftBytesLeft(result, bitShift)
		}
	}
	// shifting the remaining bits
	remainder := (decSize * ByteSize) % (bitShift * (encodeSize - 1))
	if remainder > 0 {
		result = shiftBytesLeft(result, remainder)
	}
	// discard last byte - feeder
	return result[:decSize], nil
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
