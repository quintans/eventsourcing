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
	Save(ctx context.Context, aggregate Aggregater, options Options) error
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest) error
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

type Locker interface {
	Lock() bool
	Unlock()
}

const (
	maxWait = time.Minute
)

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

type Cancel func()

type Option func(*Poller)

func PollInterval(pi time.Duration) Option {
	return func(l *Poller) {
		l.pollInterval = pi
	}
}

func StartFrom(from Start) Option {
	return func(l *Poller) {
		l.startFrom = from
	}
}

func AfterEventID(eventID string) Option {
	return func(l *Poller) {
		l.afterEventID = eventID
		l.startFrom = SEQUENCE
	}
}

func AggregateTypes(at ...string) Option {
	return func(l *Poller) {
		l.aggregateTypes = at
	}
}

func Limit(limit int) Option {
	return func(l *Poller) {
		if limit > 0 {
			l.limit = limit
		}
	}
}

func NewPoller(est Tracker, locker Locker, options ...Option) *Poller {
	p := &Poller{
		est:          est,
		pollInterval: 500 * time.Millisecond,
		startFrom:    END,
		limit:        20,
		locker:       locker,
	}
	for _, o := range options {
		o(p)
	}
	return p
}

type Poller struct {
	est            Tracker
	pollInterval   time.Duration
	startFrom      Start
	afterEventID   string
	aggregateTypes []string
	limit          int
	locker         Locker
}

func (p *Poller) Handle(ctx context.Context, handler func(ctx context.Context, e Event)) (Cancel, error) {
	var afterEventID string
	var err error
	switch p.startFrom {
	case END:
		afterEventID, err = p.est.GetLastEventID(ctx)
		if err != nil {
			return nil, err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = p.afterEventID
	}

	done := make(chan bool)

	cancel := func() {
		done <- true
	}

	go func() {
		wait := p.pollInterval
		for {
			select {
			case <-done:
				return
			case _ = <-time.After(p.pollInterval):
				if p.locker.Lock() {
					defer p.locker.Unlock()

					eid, err := p.retrieve(ctx, handler, afterEventID, "")
					if err != nil {
						wait += 2 * wait
						if wait > maxWait {
							wait = maxWait
						}
						log.WithField("backoff", wait).
							WithError(err).
							Error("Failure retrieving events. Backing off.")
					} else {
						afterEventID = eid
						wait = p.pollInterval
					}
				}
			}
		}
	}()

	return cancel, nil
}

func (p *Poller) ReplayUntil(ctx context.Context, handler func(ctx context.Context, e Event), untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, "", untilEventID)
}

func (p *Poller) ReplayFromUntil(ctx context.Context, handler func(ctx context.Context, e Event), afterEventID, untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, afterEventID, untilEventID)
}

func (p *Poller) retrieve(ctx context.Context, handler func(ctx context.Context, e Event), afterEventID, untilEventID string) (string, error) {
	loop := true
	for loop {
		events, err := p.est.GetEvents(ctx, afterEventID, p.limit)
		if err != nil {
			return "", err
		}
		for _, evt := range events {
			handler(ctx, evt)
			afterEventID = evt.ID
			if evt.ID == untilEventID {
				return untilEventID, nil
			}
		}
		loop = len(events) == p.limit
	}
	return afterEventID, nil
}
