package eventstore

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type EventStore interface {
	GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error
	Save(ctx context.Context, aggregate Aggregater) error
}

type Tracker interface {
	GetLastEventID(ctx context.Context) (string, error)
	GetEventsForAggregate(ctx context.Context, afterEventID string, aggregateID string, limit int) ([]Event, error)
	GetEvents(ctx context.Context, afterEventID string, limit int, aggregateTypes ...string) ([]Event, error)
}

type Aggregater interface {
	GetID() string
	GetVersion() int
	SetVersion(int)
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
}

type Event struct {
	ID               string
	AggregateID      string
	AggregateVersion int
	AggregateType    string
	Kind             string
	Body             []byte
	CreatedAt        time.Time
}

const (
	maxFailures = 3
)

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

type Cancel func()

type Option func(*ListenerManager)

func PollInterval(pi time.Duration) Option {
	return func(l *ListenerManager) {
		l.pollInterval = pi
	}
}

func NewListenerManager(est Tracker, options ...Option) *ListenerManager {
	t := &ListenerManager{
		est:           est,
		pollInterval:  500 * time.Millisecond,
		cancellations: []Cancel{},
	}
	for _, o := range options {
		o(t)
	}
	return t
}

type ListenerManager struct {
	est           Tracker
	pollInterval  time.Duration
	cancellations []Cancel
}

func (lm *ListenerManager) Close() {
	for _, c := range lm.cancellations {
		c()
	}
}

func (lm *ListenerManager) Listen(handler func(e Event), options ...ListenerOption) (Cancel, error) {
	l := &Listener{
		limit: 100,
	}
	for _, o := range options {
		o(l)
	}

	var afterEventID string
	var err error
	switch l.startFrom {
	case END:
		afterEventID, err = lm.est.GetLastEventID(context.Background())
		if err != nil {
			return nil, err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = l.afterEventID
	}

	ticker := time.NewTicker(lm.pollInterval)
	done := make(chan bool)

	cancel := func() {
		done <- true
		ticker.Stop()
	}
	lm.cancellations = append(lm.cancellations, cancel)

	go func() {
		failedCounter := 0
		for {
			select {
			case <-done:
				return
			case _ = <-ticker.C:
				eid, err := lm.retrieve(handler, afterEventID, l.limit)
				if err != nil {
					log.WithError(err).Error("Failure retrieving events")
					failedCounter++
					if failedCounter == maxFailures {
						log.
							WithField("aggregate_types", l.aggregateTypes).
							Errorf("Stop listening after %d consecutive failures", maxFailures)
						return
					}
				} else {
					afterEventID = eid
					failedCounter = 0
				}
			}
		}
	}()

	return cancel, nil
}

func (lm *ListenerManager) retrieve(handler func(e Event), afterEventID string, limit int) (string, error) {
	events, err := lm.est.GetEvents(context.Background(), afterEventID, limit)
	if err != nil {
		return "", err
	}
	for _, evt := range events {
		handler(evt)
		afterEventID = evt.ID
	}
	return afterEventID, nil
}

type ListenerOption func(l *Listener)

func StartFrom(from Start) ListenerOption {
	return func(l *Listener) {
		l.startFrom = from
	}
}

func AfterEventID(eventID string) ListenerOption {
	return func(l *Listener) {
		l.afterEventID = eventID
		l.startFrom = SEQUENCE
	}
}

func AggregateTypes(at ...string) ListenerOption {
	return func(l *Listener) {
		l.aggregateTypes = at
	}
}

func Limit(limit int) ListenerOption {
	return func(l *Listener) {
		if limit > 0 {
			l.limit = limit
		}
	}
}

type Listener struct {
	startFrom      Start
	afterEventID   string
	aggregateTypes []string
	limit          int
}
