package eventsourcing

import (
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/util"
)

func RehydrateAggregate(decoder Decoder, aggregateKind Kind, body []byte) (Aggregater, error) {
	a, err := rehydrate(aggregateKind, decoder, body, false)
	if err != nil {
		return nil, err
	}
	return a.(Aggregater), nil
}

func RehydrateEvent(decoder Decoder, kind Kind, body []byte) (Kinder, error) {
	return rehydrate(kind, decoder, body, true)
}

func rehydrate(kind Kind, decoder Decoder, body []byte, dereference bool) (Kinder, error) {
	var err error
	var e interface{}
	e, err = decoder.Decode(body, kind)
	if err != nil {
		return nil, faults.Errorf("Unable to decode into %T: %w", e, err)
	}

	if dereference {
		e2 := util.Dereference(e)
		return e2.(Kinder), nil
	}

	return e.(Kinder), nil
}
