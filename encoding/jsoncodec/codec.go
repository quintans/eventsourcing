package jsoncodec

import (
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
)

func init() {
	extra.SupportPrivateFields()
	extra.SetNamingStrategy(func(s string) string {
		first := unicode.ToLower(rune(s[0]))
		return string(first) + s[1:]
	})
}

type (
	Factory[K eventsourcing.ID] func(id K) eventsourcing.Kinder
	Upcaster                    func(t eventsourcing.Kinder) (eventsourcing.Kinder, error)
)

func New[K eventsourcing.ID]() *Codec[K] {
	return &Codec[K]{
		factories: map[eventsourcing.Kind]Factory[K]{},
		upcasters: map[eventsourcing.Kind]Upcaster{},
	}
}

type Codec[K eventsourcing.ID] struct {
	factories map[eventsourcing.Kind]Factory[K]
	upcasters map[eventsourcing.Kind]Upcaster
}

func (j *Codec[K]) RegisterFactory(kind eventsourcing.Kind, factory Factory[K]) {
	j.factories[kind] = factory
}

func (j *Codec[K]) RegisterUpcaster(kind eventsourcing.Kind, upcaster Upcaster) {
	j.upcasters[kind] = upcaster
}

func (Codec[K]) Encode(v eventsourcing.Kinder) ([]byte, error) {
	b, err := jsoniter.Marshal(v)
	return b, faults.Wrap(err)
}

func (j Codec[K]) Decode(data []byte, meta eventsourcing.DecoderMeta[K]) (eventsourcing.Kinder, error) {
	factory := j.factories[meta.Kind]
	if factory == nil {
		return nil, faults.Errorf("no factory registered for kind '%s'", meta.Kind)
	}

	target := factory(meta.AggregateID)

	if len(data) == 0 {
		return target, nil
	}

	err := jsoniter.Unmarshal(data, target)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	k := meta.Kind
	upcaster := j.upcasters[k]
	for upcaster != nil {
		target, err = upcaster(target)
		if err != nil {
			return nil, faults.Errorf("failed to apply '%s' upcast: %w", k, err)
		}
		k = target.GetKind()
		upcaster = j.upcasters[k]
	}

	return target, nil
}
