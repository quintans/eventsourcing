//go:build unit

package eventsourcing

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type Created struct{}

func (Created) GetKind() Kind {
	return "created"
}

type Updated struct{}

func (Updated) GetKind() Kind {
	return "updated"
}

type Unmapped struct{}

func (Unmapped) GetKind() Kind {
	return "unmapped"
}

type Foo struct {
	RootAggregate[uuid.UUID]
}

func (Foo) GetID() uuid.UUID {
	return uuid.UUID{}
}

func (*Foo) PopEvents() []Eventer {
	return nil
}

func (*Foo) HandleEvent(Eventer) error {
	return nil
}

func (Foo) GetKind() Kind {
	return "foo"
}

func (*Foo) HandleCreated(Created) {
}

func (*Foo) HandleUpdated(Updated) {
}

func TestHandlerCall(t *testing.T) {
	tcs := map[string]struct {
		event Eventer
		err   error
	}{
		"golden": {
			event: Created{},
		},
		"no_handler": {
			event: Unmapped{},
			err:   ErrHandlerNotFound,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			f := &Foo{}
			err := HandlerCall[uuid.UUID](f, tc.event)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
