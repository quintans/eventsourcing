package eventsourcing

import (
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/util"
)

func RehydrateAggregate(decoder Decoder, aggregateType AggregateType, body []byte) (Aggregater, error) {
	a, err := rehydrate(aggregateType.String(), decoder, body, false)
	if err != nil {
		return nil, err
	}
	return a.(Aggregater), nil
}

func RehydrateEvent(decoder Decoder, kind EventKind, body []byte) (Typer, error) {
	return rehydrate(kind.String(), decoder, body, true)
}

func rehydrate(kind string, decoder Decoder, body []byte, dereference bool) (Typer, error) {
	var err error
	var e interface{}
	e, err = decoder.Decode(body, kind)
	if err != nil {
		return nil, faults.Errorf("Unable to decode into %T: %w", e, err)
	}

	if dereference {
		e2 := util.Dereference(e)
		return e2.(Typer), nil
	}

	return e.(Typer), nil
}
