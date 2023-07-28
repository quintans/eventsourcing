package projection

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

// ResumeKey is used to retrieve the last event id to replay messages directly from the event store.
type ResumeKey struct {
	// topic identifies the topic. eg: account#3
	topic util.Topic
	// projection identifies a projection name for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	projection string
}

func NewResume(topic util.Topic, projectionName string) (ResumeKey, error) {
	if projectionName == "" {
		return ResumeKey{}, faults.New("projection name cannot be empty")
	}

	return ResumeKey{
		topic:      topic,
		projection: projectionName,
	}, nil
}

func (r ResumeKey) Topic() util.Topic {
	return r.topic
}

func (r ResumeKey) Projection() string {
	return r.projection
}

func (r ResumeKey) String() string {
	return r.topic.String() + ":" + r.projection
}

type ConsumerOptions struct {
	Filter  func(e *sink.Message) bool
	AckWait time.Duration
}

type ConsumerOption func(*ConsumerOptions)

func WithFilter(filter func(e *sink.Message) bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Filter = filter
	}
}

func WithAckWait(ackWait time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.AckWait = ackWait
	}
}

type Consumer interface {
	StartConsumer(ctx context.Context, projection Projection, options ...ConsumerOption) error
	Topic() util.Topic
}

type Subscriber interface {
	Consumer
	RetrieveLastSequence(ctx context.Context) (uint64, error)
}

type Event struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Kind             eventsourcing.Kind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         *encoding.JSON
	CreatedAt        time.Time
}

func FromEvent(e *eventsourcing.Event) *Event {
	return &Event{
		ID:               e.ID,
		AggregateID:      e.AggregateID,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
}

func FromMessage(m *sink.Message) *Event {
	return &Event{
		ID:               m.ID,
		AggregateID:      m.AggregateID,
		AggregateVersion: m.AggregateVersion,
		AggregateKind:    m.AggregateKind,
		Kind:             m.Kind,
		Body:             m.Body,
		IdempotencyKey:   m.IdempotencyKey,
		Metadata:         m.Metadata,
		CreatedAt:        m.CreatedAt,
	}
}

type TokenKind string

const (
	CatchUpToken  TokenKind = "catchup"
	ConsumerToken TokenKind = "consumer"
)

type Token struct {
	kind     TokenKind
	sequence uint64
}

func NewToken(kind TokenKind, sequence uint64) Token {
	return Token{
		kind:     kind,
		sequence: sequence,
	}
}

func ParseToken(s string) (Token, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return Token{}, faults.Errorf("invalid token format: %s", s)
	}
	k := TokenKind(parts[0])
	if !util.In(k, CatchUpToken, ConsumerToken) {
		return Token{}, faults.Errorf("invalid kind when parsing token: %s", s)
	}

	seq := parts[1]
	sequence, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return Token{}, faults.Errorf("parsing sequence token part '%s': %w", seq, err)
	}

	return Token{
		kind:     k,
		sequence: sequence,
	}, nil
}

func (t Token) String() string {
	return fmt.Sprintf("%v:%v", string(t.kind), t.sequence)
}

func (t Token) Kind() TokenKind {
	return t.kind
}

func (t Token) Sequence() uint64 {
	return t.sequence
}

func (t Token) IsEmpty() bool {
	return t.sequence == 0
}

func (t Token) IsZero() bool {
	return t == Token{}
}

var ErrResumeTokenNotFound = errors.New("resume token not found")

type ReadResumeStore interface {
	// GetStreamResumeToken retrieves the resume token for the resume key.
	// If the a resume key is not found it return ErrResumeTokenNotFound as an error
	GetStreamResumeToken(ctx context.Context, key ResumeKey) (Token, error)
}

type WriteResumeStore interface {
	SetStreamResumeToken(ctx context.Context, key ResumeKey, token Token) error
}

type ResumeStore interface {
	ReadResumeStore
	WriteResumeStore
}

type Projection interface {
	ReadResumeStore

	Name() string
	Options() Options
	Handle(ctx context.Context, meta MetaData, e *sink.Message) error
}

type (
	LockerFactory     func(lockName string) lock.Locker
	WaitLockerFactory func(lockName string) lock.WaitLocker
	CatchUpCallback   func(context.Context, eventid.EventID) (eventid.EventID, error)
)

type Options struct {
	CatchUpSafetyMargin time.Duration
	CatchUpFilters      []store.FilterOption
}

// NewProjector creates a subscriber to an event stream and process all events.
//
// It will check if it needs to do a catch up.
// If so, it will try acquire a lock and run a projection catchup.
// If it is unable to acquire the lock because it is held by another process, it will wait for its release.
// In the end it will fire up the subscribers.
// All this will happen in a separate go routine allowing the service to completely start up.
//
// After a successfully projection creation, subsequent start up will no longer execute the catch up function.
// This can be used to migrate projections, where a completely new projection will be populated.
//
// The catch up function should replay all events from all event stores needed for this
func Project(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository,
	subscriber Subscriber,
	projection Projection,
) *worker.RunWorker {
	name := fmt.Sprintf("%s-%s", projection.Name(), subscriber.Topic())
	logger = logger.WithTags(log.Tags{
		"projection": projection.Name(),
	})
	return worker.NewRunWorker(
		logger,
		name,
		projection.Name(),
		nil,
		func(ctx context.Context) error {
			err := catchUp(ctx, logger, lockerFactory, esRepo, subscriber, projection)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				logger.WithError(err).Error("catching up projection")
				return err
			}
			logger.Info("Finished catching up projection")

			err = subscriber.StartConsumer(ctx, projection)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				logger.WithError(err).Errorf("start consumer '%s' for projection %s", subscriber.Topic(), projection.Name())
				return err
			}
			return nil
		},
	)
}

// catchUp applies all events needed to catchup up to the subscription.
// If we have multiple replicas for one subscription, we will have only one catch up running.
func catchUp(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository,
	subscriber Subscriber,
	projection Projection,
) error {
	// first check without acquiring lock
	token, err := getSavedToken(ctx, subscriber, projection)
	if err != nil {
		return faults.Wrap(err)
	}
	if token.Kind() != CatchUpToken {
		return nil
	}

	if lockerFactory != nil {
		name := fmt.Sprintf("%s:%s-lock", subscriber.Topic(), projection.Name())
		locker := lockerFactory(name)

		if locker != nil {
			// lock for catchup
			ctx, err = locker.WaitForLock(ctx)
			if err != nil {
				return faults.Wrap(err)
			}

			defer func() {
				er := locker.Unlock(context.Background())
				if er != nil {
					logger.WithError(er).Error("unlock on catchUp")
				}
			}()
		}

		// recheck if we still need to do a catchup
		token, err = getSavedToken(ctx, subscriber, projection)
		if err != nil {
			return faults.Wrap(err)
		}
		if token.Kind() != CatchUpToken {
			return nil
		}
	}

	return catching(ctx, logger, esRepo, subscriber, token.Sequence(), projection)
}

func catching(
	ctx context.Context,
	logger log.Logger,
	esRepo EventsRepository,
	subscriber Subscriber,
	startAt uint64,
	projection Projection,
) error {
	logger.WithTags(log.Tags{"startAt": startAt}).Info("Catching up events")
	filter := store.Filter{}
	options := projection.Options()
	for _, option := range options.CatchUpFilters {
		option(&filter)
	}

	player := NewPlayer(esRepo)

	topic := subscriber.Topic().Root()
	handler := func(ctx context.Context, meta MetaData, e *sink.Message) error {
		// meta.Topic will be empty when replaying, so we set it to the subscriber topic
		meta.Topic = topic
		return projection.Handle(ctx, meta, e)
	}

	// loop until it is safe to switch to the subscriber
	seq := startAt
	for {
		start := time.Now()

		until, err := esRepo.GetMaxSeq(ctx, filter)
		if err != nil {
			return faults.Wrap(err)
		}

		logger.WithTags(log.Tags{"from": seq, "until": until}).Info("Replaying all events from the event store")
		// first catch up (this can take days)
		// projection.Handler should take care of saving the resume value
		seq, err = player.Replay(ctx, handler, seq, until, options.CatchUpFilters...)
		if err != nil {
			return faults.Errorf("replaying events from '%d' until '%d': %w", seq, until, err)
		}

		catchUpSafetyMargin := options.CatchUpSafetyMargin
		if catchUpSafetyMargin == 0 {
			catchUpSafetyMargin = time.Hour
		}

		// if the catch up took less than catchUpSafetyMargin we can safely exit and switch to the event bus
		if time.Since(start) < catchUpSafetyMargin {
			break
		}
	}

	// this call can hang if there is no message in the MQ but that is ok
	resume, err := subscriber.RetrieveLastSequence(ctx)
	if err != nil {
		return faults.Wrap(err)
	}

	if resume > seq {
		logger.WithTags(log.Tags{"from": seq, "until": resume}).Info("Catching up projection up to the subscription")
		// this should be very quick since the bulk of the work was already done
		seq, err = player.Replay(ctx, handler, seq, resume, options.CatchUpFilters...)
		if err != nil {
			return faults.Errorf("replaying events from '%d' until '%d': %w", seq, resume, err)
		}
	} else {
		logger.WithTags(log.Tags{"from": seq, "until": resume}).Info("Skipping catching up the projection up to the subscription. All events already handled.")
	}

	return nil
}

func getSavedToken(ctx context.Context, sub Consumer, prj Projection) (Token, error) {
	resume, err := NewResume(sub.Topic(), prj.Name())
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	token, err := prj.GetStreamResumeToken(ctx, resume)
	if errors.Is(err, ErrResumeTokenNotFound) {
		return NewToken(CatchUpToken, 0), nil
	}

	return token, faults.Wrap(err)
}
