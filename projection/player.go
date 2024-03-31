package projection

import (
	"context"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

type (
	MessageHandlerFunc[K eventsourcing.ID] func(ctx context.Context, e *sink.Message[K]) error
)

type EventsRepository[K eventsourcing.ID] interface {
	GetEvents(ctx context.Context, afterEventID eventid.EventID, untilEventID eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error)
}

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

const defEventsBatchSize = 1000

type Cancel func()

type Option[K eventsourcing.ID] func(*Player[K])

type Player[K eventsourcing.ID] struct {
	store        EventsRepository[K]
	batchSize    int
	customFilter func(*eventsourcing.Event[K]) bool
}

func WithBatchSize[K eventsourcing.ID](batchSize int) Option[K] {
	return func(p *Player[K]) {
		if batchSize > 0 {
			p.batchSize = batchSize
		}
	}
}

func WithCustomFilter[K eventsourcing.ID](fn func(events *eventsourcing.Event[K]) bool) Option[K] {
	return func(p *Player[K]) {
		p.customFilter = fn
	}
}

// NewPlayer instantiates a new Player.
func NewPlayer[K eventsourcing.ID](repository EventsRepository[K], options ...Option[K]) Player[K] {
	p := Player[K]{
		store:     repository,
		batchSize: defEventsBatchSize,
	}

	for _, f := range options {
		f(&p)
	}

	return p
}

type StartOption struct {
	startFrom Start
	afterSeq  uint64
}

func (so StartOption) StartFrom() Start {
	return so.startFrom
}

func (so StartOption) AfterSeq() uint64 {
	return so.afterSeq
}

func StartEnd() StartOption {
	return StartOption{
		startFrom: END,
	}
}

func StartBeginning() StartOption {
	return StartOption{
		startFrom: BEGINNING,
	}
}

func StartAt(sequence uint64) StartOption {
	return StartOption{
		startFrom: SEQUENCE,
		afterSeq:  sequence,
	}
}

func (p Player[K]) Replay(ctx context.Context, handler MessageHandlerFunc[K], afterEventID, untilEventID eventid.EventID, filters ...store.FilterOption) (eventid.EventID, int64, error) {
	ch1 := getEvents(ctx, p.store, afterEventID, untilEventID, p.batchSize, filters...)
	r := <-handleEvents(ctx, ch1, afterEventID, handler, p.customFilter)
	if r.Error != nil {
		return eventid.Zero, 0, faults.Wrap(r.Error)
	}

	return r.AfterEventID, r.Count, nil
}

type GetEventsReply[K eventsourcing.ID] struct {
	Events []*eventsourcing.Event[K]
	Error  error
}

func getEvents[K eventsourcing.ID](
	ctx context.Context,
	eventStore EventsRepository[K],
	afterEventID eventid.EventID,
	untilEventID eventid.EventID,
	batchSize int,
	filters ...store.FilterOption,
) <-chan GetEventsReply[K] {
	out := make(chan GetEventsReply[K])
	go func() {
		defer close(out)

		filter := store.Filter{}
		for _, f := range filters {
			f(&filter)
		}

		for {
			events, err := eventStore.GetEvents(ctx, afterEventID, untilEventID, batchSize, filter)
			if err != nil {
				out <- GetEventsReply[K]{Error: err}
				return
			}

			if len(events) == 0 {
				return
			}

			out <- GetEventsReply[K]{Events: events}

			afterEventID = events[len(events)-1].ID

			if len(events) != batchSize || untilEventID != eventid.Zero && afterEventID.Compare(untilEventID) >= 0 {
				return
			}
		}
	}()
	return out
}

type HandleEventsReply[K eventsourcing.ID] struct {
	AfterEventID eventid.EventID
	Count        int64
	Error        error
}

func handleEvents[K eventsourcing.ID](
	ctx context.Context,
	input <-chan GetEventsReply[K],
	afterEventID eventid.EventID,
	handler MessageHandlerFunc[K],
	customFilter func(*eventsourcing.Event[K]) bool,
) <-chan HandleEventsReply[K] {
	out := make(chan HandleEventsReply[K])
	go func() {
		var err error
		var count int64
		defer func() {
			if err != nil {
				out <- HandleEventsReply[K]{Error: err}
			} else {
				out <- HandleEventsReply[K]{AfterEventID: afterEventID, Count: count}
			}
			close(out)
		}()

		for in := range input {
			if in.Error != nil {
				err = in.Error
				return
			}

			for _, evt := range in.Events {
				if customFilter == nil || customFilter(evt) {
					err = handler(ctx, sink.ToMessage(evt))
					if err != nil {
						return
					}
				}
			}
			count += int64(len(in.Events))
			afterEventID = in.Events[len(in.Events)-1].ID
		}
	}()
	return out
}
