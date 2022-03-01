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

type Upcaster func(t eventsourcing.Kinder) (eventsourcing.Kinder, error)

func New() *Codec {
	return &Codec{
		factories: map[eventsourcing.Kind]func() eventsourcing.Kinder{},
		upcasters: map[eventsourcing.Kind]Upcaster{},
	}
}

type Codec struct {
	factories map[eventsourcing.Kind]func() eventsourcing.Kinder
	upcasters map[eventsourcing.Kind]Upcaster
}

func (j *Codec) RegisterFactory(kind eventsourcing.Kind, factory func() eventsourcing.Kinder) {
	j.factories[kind] = factory
}

func (j *Codec) RegisterUpcaster(kind eventsourcing.Kind, upcaster Upcaster) {
	j.upcasters[kind] = upcaster
}

func (Codec) Encode(v eventsourcing.Kinder) ([]byte, error) {
	b, err := jsoniter.Marshal(v)
	return b, faults.Wrap(err)
}

func (j Codec) Decode(data []byte, kind eventsourcing.Kind) (eventsourcing.Kinder, error) {
	factory := j.factories[kind]
	if factory == nil {
		return nil, faults.Errorf("no factory registered for kind '%s'", kind)
	}

	target := factory()

	if len(data) == 0 {
		return target, nil
	}

	err := jsoniter.Unmarshal(data, target)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	k := kind
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
