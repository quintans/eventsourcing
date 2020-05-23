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

type Options struct {
	IdempotencyKey string
}

type EventStore interface {
	GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error
	Save(ctx context.Context, aggregate Aggregater, options Options) (bool, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
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
	IdempotencyKey   string
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

type Option func(*Listener)

func PollInterval(pi time.Duration) Option {
	return func(l *Listener) {
		l.pollInterval = pi
	}
}

func StartFrom(from Start) Option {
	return func(l *Listener) {
		l.startFrom = from
	}
}

func AfterEventID(eventID string) Option {
	return func(l *Listener) {
		l.afterEventID = eventID
		l.startFrom = SEQUENCE
	}
}

func AggregateTypes(at ...string) Option {
	return func(l *Listener) {
		l.aggregateTypes = at
	}
}

func Limit(limit int) Option {
	return func(l *Listener) {
		if limit > 0 {
			l.limit = limit
		}
	}
}

func NewListener(est Tracker, options ...Option) *Listener {
	l := &Listener{
		est:          est,
		pollInterval: 500 * time.Millisecond,
		startFrom:    END,
		limit:        100,
	}
	for _, o := range options {
		o(l)
	}
	return l
}

type Listener struct {
	est            Tracker
	pollInterval   time.Duration
	startFrom      Start
	afterEventID   string
	aggregateTypes []string
	limit          int
}

func (l *Listener) Listen(handler func(e Event)) (Cancel, error) {
	var afterEventID string
	var err error
	switch l.startFrom {
	case END:
		afterEventID, err = l.est.GetLastEventID(context.Background())
		if err != nil {
			return nil, err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = l.afterEventID
	}

	ticker := time.NewTicker(l.pollInterval)
	done := make(chan bool)

	cancel := func() {
		done <- true
		ticker.Stop()
	}

	go func() {
		failedCounter := 0
		for {
			select {
			case <-done:
				return
			case _ = <-ticker.C:
				eid, err := l.retrieve(handler, afterEventID, l.limit)
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

func (l *Listener) retrieve(handler func(e Event), afterEventID string, limit int) (string, error) {
	events, err := l.est.GetEvents(context.Background(), afterEventID, limit)
	if err != nil {
		return "", err
	}
	for _, evt := range events {
		handler(evt)
		afterEventID = evt.ID
	}
	return afterEventID, nil
}
