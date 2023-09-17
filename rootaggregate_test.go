package eventsourcing_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/quintans/eventsourcing"
	"github.com/stretchr/testify/assert"
)

type Created struct{}

func (Created) GetKind() eventsourcing.Kind {
	return ""
}

type Updated struct{}

func (Updated) GetKind() eventsourcing.Kind {
	return ""
}

type Unmapped struct{}

func (Unmapped) GetKind() eventsourcing.Kind {
	return ""
}

type Foo struct{}

func (Foo) GetID() uuid.UUID {
	return uuid.UUID{}
}

func (*Foo) PopEvents() []eventsourcing.Eventer {
	return nil
}

func (*Foo) HandleEvent(eventsourcing.Eventer) error {
	return nil
}

func (Foo) GetKind() eventsourcing.Kind {
	return ""
}

func (*Foo) HandleCreated(Created) error {
	return nil
}

var myError = errors.New("ERROR!")

func (*Foo) HandleUpdated(Updated) error {
	return myError
}

func TestHandlerCall(t *testing.T) {
	tcs := map[string]struct {
		event eventsourcing.Eventer
		err   error
	}{
		"golden": {
			event: Created{},
		},
		"returning_error": {
			event: Updated{},
			err:   myError,
		},
		"no_handler": {
			event: Unmapped{},
			err:   eventsourcing.ErrHandlerNotFound,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			f := &Foo{}
			err := eventsourcing.HandlerCall[uuid.UUID](f, tc.event)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
