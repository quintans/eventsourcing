package eventsourcing

import (
	"github.com/quintans/faults"
)

func RehydrateAggregate[T Aggregater](decoder Decoder, aggregateKind Kind, body []byte) (T, error) {
	a, err := rehydrate(aggregateKind, decoder, body, false)
	if err != nil {
		var zero T
		return zero, err
	}
	return a.(T), nil
}

func RehydrateEvent(decoder Decoder, kind Kind, body []byte) (Kinder, error) {
	return rehydrate(kind, decoder, body, true)
}

func rehydrate(kind Kind, decoder Decoder, body []byte, dereference bool) (Kinder, error) {
	e, err := decoder.Decode(body, kind)
	if err != nil {
		return nil, faults.Errorf("Unable to decode into %T: %w", e, err)
	}

	return e, nil
}
