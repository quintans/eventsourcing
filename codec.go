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

func RehydrateAggregate(factory AggregateFactory, decoder Decoder, upcaster Upcaster, aggregateType AggregateType, body []byte) (Aggregater, error) {
	e, err := factory.NewAggregate(aggregateType)
	if err != nil {
		return nil, err
	}
	a, err := rehydrate(e, decoder, upcaster, body, false)
	if err != nil {
		return nil, err
	}
	return a.(Aggregater), nil
}

func RehydrateEvent(factory EventFactory, decoder Decoder, upcaster Upcaster, kind EventKind, body []byte) (Typer, error) {
	e, err := factory.NewEvent(kind)
	if err != nil {
		return nil, err
	}
	return rehydrate(e, decoder, upcaster, body, true)
}

func rehydrate(e Typer, decoder Decoder, upcaster Upcaster, body []byte, dereference bool) (Typer, error) {
	if len(body) > 0 {
		err := decoder.Decode(body, e)
		if err != nil {
			return nil, faults.Errorf("Unable to decode into %T: %w", e, err)
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
