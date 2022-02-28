package jsoncodec

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
	"github.com/quintans/faults"
)

func init() {
	extra.SupportPrivateFields()
}

func New() *Codec {
	return &Codec{
		factories: map[string]func() interface{}{},
	}
}

type Codec struct {
	factories map[string]func() interface{}
}

func (j *Codec) RegisterFactory(kind string, factory func() interface{}) {
	j.factories[kind] = factory
}

func (Codec) Encode(v interface{}) ([]byte, error) {
	b, err := jsoniter.Marshal(v)
	return b, faults.Wrap(err)
}

func (j Codec) Decode(data []byte, kind string) (interface{}, error) {
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
	return target, nil
}
