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

func NewStreamResume(topic util.Topic, stream string) (ResumeKey, error) {
	if stream == "" {
		return ResumeKey{}, faults.New("stream cannot be empty")
	}

	return ResumeKey{
		topic:      topic,
		projection: stream,
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
	StartConsumer(ctx context.Context, handler MessageHandlerFunc, options ...ConsumerOption) error
	StopConsumer(ctx context.Context)
}

type Subscriber interface {
	Consumer
	ResumeKey() ResumeKey
	RetrieveLastSequence(ctx context.Context) (uint64, error)
	SaveLastSequence(ctx context.Context, token uint64) error
}

type Event struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateIDHash  uint32
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
		AggregateIDHash:  e.AggregateIDHash,
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
		AggregateIDHash:  m.AggregateIDHash,
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
	kind  TokenKind
	token uint64
}

func NewToken(kind TokenKind, token uint64) Token {
	return Token{
		kind:  kind,
		token: token,
	}
}

func ParseToken(s string) (Token, error) {
	idx := strings.Index(s, ":")
	if idx == -1 {
		return Token{}, faults.Errorf("separator not found when parsing token: %s", s)
	}
	k := TokenKind(s[:idx])
	if !util.In(k, CatchUpToken, ConsumerToken) {
		return Token{}, faults.Errorf("invalid kind when parsing token: %s", s)
	}

	t := s[idx:]
	seq, err := strconv.ParseUint(t, 10, 64)
	if err != nil {
		return Token{}, faults.Errorf("parsing token '%s': %w", t, err)
	}

	return Token{
		kind:  k,
		token: seq,
	}, nil
}

func (t Token) String() string {
	return fmt.Sprintf("%v:%v", string(t.kind), t.token)
}

func (t Token) Kind() TokenKind {
	return t.kind
}

func (t Token) Value() uint64 {
	return t.token
}

func (t Token) IsEmpty() bool {
	return t.token == 0
}

func (t Token) IsZero() bool {
	return t == Token{}
}

var ErrResumeTokenNotFound = errors.New("resume token not found")

type ResumeStore interface {
	// GetStreamResumeToken retrieves the resume token for the resume key.
	// If the a resume key is not found it return ErrResumeTokenNotFound as an error
	GetStreamResumeToken(ctx context.Context, key ResumeKey) (Token, error)
	SetStreamResumeToken(ctx context.Context, key ResumeKey, token Token) error
}

type (
	LockerFactory     func(lockName string) lock.Locker
	WaitLockerFactory func(lockName string) lock.WaitLocker
	CatchUpCallback   func(context.Context, eventid.EventID) (eventid.EventID, error)
)

type ProjectorOptions struct {
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
func NewProjector(
	ctx context.Context,
	logger log.Logger,
	workerLockerFactory LockerFactory,
	catchUpLockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	subscriber Subscriber,
	esRepo Repository,
	handler MessageHandlerFunc,
	options ProjectorOptions,
) *worker.RunWorker {
	rk := subscriber.ResumeKey()
	logger = logger.WithTags(log.Tags{
		"projection": rk.Projection(),
	})

	name := rk.String() + "-lock"
	catchUpLocker := catchUpLockerFactory(name)
	return worker.NewRunWorker(
		logger,
		name,
		rk.Projection(),
		workerLockerFactory(name),
		worker.NewTask(
			func(ctx context.Context) error {
				err := catchUp(ctx, logger, catchUpLocker, resumeStore, subscriber, esRepo, handler, options)
				if err != nil {
					logger.WithError(err).Error("catchup projection '%s'", rk.Projection())
				}
				logger.Info("Finished catching up")

				err = subscriber.StartConsumer(ctx, handler)
				return faults.Wrapf(err, "start consumer for %s", rk.String())
			},
			func(ctx context.Context, hard bool) {
				subscriber.StopConsumer(ctx)
			},
		),
	)
}

// catchUp applies all events needed to catchup up to the subscription.
// If we have multiple replicas for one subscription, we will have only one catch up running.
func catchUp(
	ctx context.Context,
	logger log.Logger,
	locker lock.WaitLocker,
	resumeStore ResumeStore,
	subscriber Subscriber,
	esRepo Repository,
	handler MessageHandlerFunc,
	options ProjectorOptions,
) error {
	token, err := getSavedToken(ctx, resumeStore, subscriber.ResumeKey())
	if err != nil {
		return faults.Wrap(err)
	}

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

	resumeKey := subscriber.ResumeKey()

	// recheck if we still need to do a catchup
	token, err = getSavedToken(ctx, resumeStore, resumeKey)
	if err != nil {
		return faults.Wrap(err)
	}
	if token.Kind() != CatchUpToken {
		return nil
	}

	handler = func(ctx context.Context, meta Meta, e *sink.Message) error {
		err := handler(ctx, meta, e)
		if err != nil {
			return err
		}
		return resumeStore.SetStreamResumeToken(ctx, resumeKey, NewToken(CatchUpToken, meta.Sequence))
	}

	seq, err := catching(ctx, logger, subscriber, esRepo, token.Value(), handler, options)

	logger.Info("Recording subscriptions positions")
	err = recordResumeToken(ctx, subscriber, seq)
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

func catching(
	ctx context.Context,
	logger log.Logger,
	subscriber Subscriber,
	esRepo Repository,
	startAt uint64,
	handler MessageHandlerFunc,
	options ProjectorOptions,
) (uint64, error) {
	logger.Info("Retrieving subscriptions last position for the first run")
	filter := store.Filter{}
	for _, option := range options.CatchUpFilters {
		option(&filter)
	}

	p := New(esRepo)

	// loop until it is safe to switch to the subscriber
	var seq uint64
	for {
		start := time.Now()

		until, err := esRepo.GetMaxSeq(ctx, filter)
		if err != nil {
			return 0, faults.Wrap(err)
		}

		// first catch up (this can take days)
		// catchUpCallback should take care of saving the resume value
		seq, err = p.ReplayFromUntil(ctx, handler, startAt, until, options.CatchUpFilters...)
		if err != nil {
			return 0, faults.Errorf("replaying events from '%d' until '%d': %w", startAt, until, err)
		}

		catchUpSafetyMargin := options.CatchUpSafetyMargin
		if catchUpSafetyMargin == 0 {
			catchUpSafetyMargin = time.Hour
		}

		if time.Since(start) < catchUpSafetyMargin {
			break
		}
	}

	logger.Info("Retrieving subscriptions last position for the second run")
	// this call can hang if there is no message in the MQ but that is ok
	resume, err := subscriber.RetrieveLastSequence(ctx)
	if err != nil {
		return 0, faults.Wrap(err)
	}

	if resume != seq {
		logger.Info("Catching up projection for the second run")
		// this should be very quick since the bulk of the work was already done
		seq, err = p.ReplayFromUntil(ctx, handler, seq, resume, options.CatchUpFilters...)
		if err != nil {
			return 0, faults.Errorf("replaying events from '%d' until '%d': %w", seq, resume, err)
		}
	}

	return seq, nil
}

func getSavedToken(ctx context.Context, resumeStore ResumeStore, resumeKey ResumeKey) (Token, error) {
	token, err := resumeStore.GetStreamResumeToken(ctx, resumeKey)
	if errors.Is(err, ErrResumeTokenNotFound) {
		return NewToken(CatchUpToken, 0), nil
	}
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	return token, nil
}

func recordResumeToken(ctx context.Context, subscriber Subscriber, token uint64) error {
	err := subscriber.SaveLastSequence(ctx, token)
	return faults.Wrap(err)
}
