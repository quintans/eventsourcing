package projection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
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
	Filter  func(e *eventsourcing.Event) bool
	AckWait time.Duration
}

type ConsumerOption func(*ConsumerOptions)

func WithFilter(filter func(e *eventsourcing.Event) bool) ConsumerOption {
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
	StartConsumer(ctx context.Context, handler EventHandlerFunc, options ...ConsumerOption) error
	StopConsumer(ctx context.Context)
}

type Subscriber interface {
	Consumer
	ResumeKey() ResumeKey
	RetrieveLastResume(ctx context.Context) (Resume, error)
	RecordLastResume(ctx context.Context, token Token) error
}

type EventHandlerFunc func(ctx context.Context, e *eventsourcing.Event) error

type TokenKind string

const (
	CatchUpToken  TokenKind = "catchup"
	ConsumerToken TokenKind = "consumer"
)

type Token struct {
	kind  TokenKind
	token string
}

func NewToken(kind TokenKind, token string) Token {
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
	return Token{
		kind:  k,
		token: s[idx:],
	}, nil
}

func (t Token) String() string {
	return fmt.Sprintf("%s:%s", string(t.kind), t.token)
}

func (t Token) Kind() TokenKind {
	return t.kind
}

func (t Token) Value() string {
	return t.token
}

func (t Token) IsEmpty() bool {
	return t.token == ""
}

func (t Token) IsZero() bool {
	return t == Token{}
}

type Resume struct {
	Topic   util.Topic
	EventID eventid.EventID
	Token   Token
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

// NewProjector creates a subscriber to an event stream and process all events.
//
// It will check if it needs to do a catch up.
// If so, it will try acquire a lock and run a projection catchup.
// If it is unable to acquire the lock because it is held by another process, it will wait for its release.
// In the end it will fire up the subscribers.
// All this will happen in a separate go routine allowing the service to completely start up.
//
// After a successfully projection creation, subsequent start up will no longer execute the catch up function. This can be used to migrate projections,
// where a completely new projection will be populated.
//
// The catch up function should replay all events from all event stores needed for this
func NewProjector(
	ctx context.Context,
	logger log.Logger,
	workerLockerFactory LockerFactory,
	catchUpLockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	subscriber Subscriber,
	catchUpCallback CatchUpCallback,
	handler EventHandlerFunc,
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
				if catchUpCallback != nil {
					err := catchUp(ctx, logger, catchUpLocker, resumeStore, subscriber, catchUpCallback)
					if err != nil {
						logger.WithError(err).Error("catchup projection '%s'", rk.Projection())
					}
					logger.Info("Finished catching up")
				}
				err := subscriber.StartConsumer(ctx, handler)
				return faults.Wrapf(err, "start consumer for %s", rk.String())
			},
			func(ctx context.Context, hard bool) {
				subscriber.StopConsumer(ctx)
			},
		),
	)
}

func catchUp(
	ctx context.Context,
	logger log.Logger,
	locker lock.WaitLocker,
	resumeStore ResumeStore,
	subscriber Subscriber,
	catchUpCallback CatchUpCallback,
) error {
	token, err := getSavedToken(ctx, resumeStore, subscriber.ResumeKey())
	if err != nil {
		return faults.Wrap(err)
	}
	if token.Kind() != CatchUpToken {
		return nil
	}

	// lock for catchup
	for {
		_, err = locker.Lock(ctx)
		if errors.Is(err, lock.ErrLockAlreadyAcquired) {
			er := locker.WaitForUnlock(ctx)
			if er != nil {
				logger.WithError(er).Error("waiting for unlock on catchUp")
			}
			continue
		} else if err != nil {
			return faults.Wrap(err)
		}
		break
	}

	defer func() {
		er := locker.Unlock(context.Background())
		if er != nil {
			logger.WithError(er).Error("unlock on catchUp")
		}
	}()

	// recheck if we still need to do a catchup
	token, err = getSavedToken(ctx, resumeStore, subscriber.ResumeKey())
	if err != nil {
		return faults.Wrap(err)
	}
	if token.Kind() != CatchUpToken {
		return nil
	}

	logger = logger.WithTags(log.Tags{
		"method": "Projector.catchUp",
	})

	logger.Info("Catching up projection for the first time")
	id, err := eventid.Parse(token.Value())
	if err != nil {
		return faults.Errorf("parsing event ID on catchup '%s': %w", token.Value(), err)
	}
	// first catch up (this can take days)
	// catchUpCallback should take care of saving the resume value
	id, err = catchUpCallback(ctx, id)
	if err != nil {
		return faults.Errorf("executing catch up function for projection: %w", err)
	}

	logger.Info("Retrieving subscriptions last position")
	// this call can hang if there is not message in the MQ but that is ok
	resume, err := retrieveResumeFromSubscriber(ctx, subscriber)
	if err != nil {
		return faults.Wrap(err)
	}

	logger.Info("Catching up projection for the second time")
	// this should be very quick since the bulk of the work was already done
	_, err = catchUpCallback(ctx, id)
	if err != nil {
		return faults.Errorf("executing catch up function for projection: %w", err)
	}

	logger.Info("Recording subscriptions positions")
	err = recordResumeToken(ctx, subscriber, resume.Token)
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

func getSavedToken(ctx context.Context, resumeStore ResumeStore, resumeKey ResumeKey) (Token, error) {
	token, err := resumeStore.GetStreamResumeToken(ctx, resumeKey)
	if errors.Is(err, ErrResumeTokenNotFound) {
		return NewToken(CatchUpToken, eventid.Zero.String()), nil
	}
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	return token, nil
}

func retrieveResumeFromSubscriber(ctx context.Context, subscriber Subscriber) (Resume, error) {
	var max time.Time
	resume, err := subscriber.RetrieveLastResume(ctx)
	if err != nil {
		return Resume{}, faults.Wrap(err)
	}
	// add offset to compensate clock skews and the safety margin used on catch up
	resume.EventID = resume.EventID.OffsetTime(time.Second)

	// max time
	t := resume.EventID.Time()
	if t.After(max) {
		max = t
	}

	// wait for the safety offset to have passed
	now := time.Now()
	if max.After(now) {
		time.Sleep(max.Sub(now))
	}

	return resume, nil
}

func recordResumeToken(ctx context.Context, subscriber Subscriber, token Token) error {
	err := subscriber.RecordLastResume(ctx, token)
	return faults.Wrap(err)
}
