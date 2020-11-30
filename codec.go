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

func Decode(factory Factory, decoder Decoder, upcaster Upcaster, kind string, body []byte, dereference bool) (Typer, error) {
	e, err := factory.New(kind)
	if err != nil {
		return nil, err
	}
	if len(body) > 0 {
		err = decoder.Decode(body, e)
		if err != nil {
			return nil, fmt.Errorf("Unable to decode event %s: %w", kind, err)
		}
	}
	if upcaster != nil {
		e = upcaster.Upcast(e)
	}

	if dereference {
		e2 := common.Dereference(e)
		return e2.(Typer), nil
	}

	return e, nil
}
