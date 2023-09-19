package jsoncodec

import (
	"testing"

	"github.com/google/uuid"
	"github.com/quintans/eventsourcing"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	eventsourcing.RootAggregate[uuid.UUID]

	firstName string
	Code      int
}

func (*Foo) GetKind() eventsourcing.Kind {
	return "foo"
}

func TestEncode(t *testing.T) {
	reg := eventsourcing.NewRegistry()
	f := &Foo{
		RootAggregate: eventsourcing.NewRootAggregate(reg, uuid.New()),
		firstName:     "Paulo",
	}

	c := New[uuid.UUID]()
	b, err := c.Encode(f)
	require.NoError(t, err)
	require.Equal(t, `{"firstName":"Paulo","code":0}`, string(b))
}
