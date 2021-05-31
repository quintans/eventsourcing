package eventsourcing

import (
	"encoding/json"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/common"
)

type JSONCodec struct{}

func (JSONCodec) Encode(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	return b, faults.Wrap(err)
}

func (JSONCodec) Decode(data []byte, v interface{}) error {
	err := json.Unmarshal(data, v)
	return faults.Wrap(err)
}

func RehydrateAggregate(factory Factory, decoder Decoder, upcaster Upcaster, aggregateType AggregateType, body []byte) (Typer, error) {
	return rehydrate(factory, decoder, upcaster, aggregateType.String(), body, false)
}

func RehydrateEvent(factory Factory, decoder Decoder, upcaster Upcaster, kind EventKind, body []byte) (Typer, error) {
	return rehydrate(factory, decoder, upcaster, kind.String(), body, true)
}

func rehydrate(factory Factory, decoder Decoder, upcaster Upcaster, kind string, body []byte, dereference bool) (Typer, error) {
	e, err := factory.New(kind)
	if err != nil {
		return nil, err
	}
	if len(body) > 0 {
		err = decoder.Decode(body, e)
		if err != nil {
			return nil, faults.Errorf("Unable to decode event %s: %w", kind, err)
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
