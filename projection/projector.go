package projection

import (
	"context"
	"errors"
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
	// stream identifies a stream for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	stream string
}

func NewStreamResume(topic util.Topic, stream string) (ResumeKey, error) {
	if stream == "" {
		return ResumeKey{}, faults.New("stream cannot be empty")
	}

	return ResumeKey{
		topic:  topic,
		stream: stream,
	}, nil
}

func (ts ResumeKey) Topic() util.Topic {
	return ts.topic
}

func (ts ResumeKey) String() string {
	return ts.topic.String() + ":" + ts.stream
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
	StopConsumer(ctx context.Context, hard bool)
}

type Subscriber interface {
	Consumer
	ResumeKey() ResumeKey
	RetrieveLastResume(ctx context.Context) (Resume, error)
	RecordLastResume(ctx context.Context, token string) error
}

type EventHandlerFunc func(ctx context.Context, e *eventsourcing.Event) error

type Resume struct {
	Topic   util.Topic
	EventID eventid.EventID
	Token   string
}

var ErrResumeTokenNotFound = errors.New("resume token not found")

type ResumeStore interface {
	// GetStreamResumeToken retrives the resume token for the resume key.
	// If the a resume key is not found it return ErrResumeTokenNotFound as an error
	GetStreamResumeToken(ctx context.Context, key ResumeKey) (string, error)
	SetStreamResumeToken(ctx context.Context, key ResumeKey, token string) error
}

type WaitLockerFactory func(lockName string) lock.WaitLocker

// NewProjector creates subscribes to all events streams and process them.
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
	lockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	projectionName string,
	subscriber Subscriber,
	topic string,
	catchUpFunc func(context.Context, Resume) error,
	handler EventHandlerFunc,
) (*worker.RunWorker, error) {
	logger = logger.WithTags(log.Tags{
		"projection": projectionName,
	})

	name := projectionName + "-lock-" + topic
	locker := lockerFactory(name)
	return worker.NewRunWorker(
		logger,
		name,
		projectionName,
		locker,
		worker.NewTask(
			func(ctx context.Context) error {
				err := catchUp(ctx, logger, locker, resumeStore, subscriber, catchUpFunc)
				if err != nil {
					logger.WithError(err).Error("catchup projection '%s'", projectionName)
				}
				err = subscriber.StartConsumer(ctx, handler)
				return faults.Wrapf(err, "Unable to start consumer for %s-%s", projectionName, topic)
			},
			func(ctx context.Context, hard bool) {
				subscriber.StopConsumer(ctx, hard)
			},
		),
	), nil
}

func catchUp(
	ctx context.Context,
	logger log.Logger,
	locker lock.WaitLocker,
	resumeStore ResumeStore,
	subscriber Subscriber,
	catchUpFunc func(context.Context, Resume) error,
) error {
	// check if it should catch up
	ok, err := shouldCatchup(ctx, resumeStore, subscriber)
	if err != nil {
		return faults.Wrap(err)
	}
	if !ok {
		return nil
	}
	// lock
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

	// recheck if it should catch up
	ok, err = shouldCatchup(ctx, resumeStore, subscriber)
	if err != nil {
		return faults.Wrap(err)
	}
	if !ok {
		return nil
	}

	logger = logger.WithTags(log.Tags{
		"method": "Projector.catchUp",
	})

	logger.Info("Retrieving subscriptions last position")
	resume, err := retrieveResume(ctx, subscriber)
	if err != nil {
		return faults.Wrap(err)
	}

	logger.Info("Catching up projection")
	err = catchUpFunc(ctx, resume)
	if err != nil {
		return faults.Errorf("executing catch up function for projection: %w", err)
	}

	logger.Info("Recording subscriptions positions")
	err = recordResumeTokens(ctx, subscriber, resume)
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

func shouldCatchup(ctx context.Context, resumeStore ResumeStore, subscriber Subscriber) (bool, error) {
	_, err := resumeStore.GetStreamResumeToken(ctx, subscriber.ResumeKey())
	// if there is at leat one token that it is not defined it means that the last execution failed
	// and needs to be attempted again.
	if errors.Is(err, ErrResumeTokenNotFound) {
		return true, nil
	}
	if err != nil {
		return false, faults.Wrap(err)
	}

	return false, nil
}

func retrieveResume(ctx context.Context, subscriber Subscriber) (Resume, error) {
	var max time.Time
	resume, err := subscriber.RetrieveLastResume(ctx)
	if err != nil {
		return Resume{}, faults.Wrap(err)
	}
	// add offset to compensate clock skews
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

func recordResumeTokens(ctx context.Context, subscriber Subscriber, resume Resume) error {
	err := subscriber.RecordLastResume(ctx, resume.Token)
	return faults.Wrap(err)
}
