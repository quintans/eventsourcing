package ids

import (
	"database/sql/driver"
	"errors"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"
)

var Zero AggID

type AggID ulid.ULID

func New() AggID {
	return AggID(ulid.Make())
}

func Parse(s string) (AggID, error) {
	id, err := ulid.Parse(s)
	return AggID(id), err
}

func (id AggID) String() string {
	return ulid.ULID(id).String()
}

func (id *AggID) UnmarshalText(v []byte) error {
	var u ulid.ULID
	err := u.UnmarshalText([]byte(v))
	if err != nil {
		return faults.Wrap(err)
	}

	*id = AggID(u)
	return nil
}

func (id AggID) MarshalText() ([]byte, error) {
	return ulid.ULID(id).MarshalText()
}

func (id AggID) Value() (driver.Value, error) {
	return ulid.ULID(id).String(), nil
}

func (id *AggID) Scan(src any) error {
	switch x := src.(type) {
	case nil:
		return nil
	case string:
		return id.UnmarshalText([]byte(x))
	case []byte:
		return id.UnmarshalText(x)
	}

	return errors.New("AggID: source value must be a string or a byte slice")
}
