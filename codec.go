package eventstore

import (
	"encoding/json"
	"fmt"

	"github.com/quintans/eventstore/common"
)

type JsonCodec struct{}

func (_ JsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (_ JsonCodec) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func Decode(factory Factory, decoder Decoder, upcaster Upcaster, kind string, body []byte) (Eventer, error) {
	e, err := factory.New(kind)
	if err != nil {
		return nil, err
	}
	err = decoder.Decode(body, e)
	if err != nil {
		return nil, fmt.Errorf("Unable to decode event %s: %w", kind, err)
	}
	e = common.Dereference(e)
	e2 := e.(Eventer)
	if upcaster != nil {
		e2 = upcaster.Upcast(e2)
	}

	return e2, nil
}
