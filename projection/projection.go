package projection

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

const defaultUntilOffset = 15 * time.Minute

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
	logger log.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository,
	subscriber Consumer,
	projection Projection,
	splits int,
	resumeStore store.KVRStore,
) *worker.RunWorker {
	topic, parts := subscriber.TopicPartitions()
	joinedParts := joinUints(parts)
	name := fmt.Sprintf("%s-%s.%s", projection.Name(), topic, joinedParts)
	logger = logger.WithTags(log.Tags{
		"projection": projection.Name(),
	})

	return worker.NewRunWorker(
		logger,
		name,
		projection.Name(),
		nil,
		func(ctx context.Context) error {
			return catchUp(ctx, logger, lockerFactory, esRepo, topic, splits, subscriber, projection, resumeStore)
		},
	)
}

func joinUints(p []uint32) string {
	var sb strings.Builder
	for k, v := range p {
		if k > 0 {
			sb.WriteString("_")
		}
		sb.WriteString(strconv.Itoa(int(v)))

	}
	return sb.String()
}

// catchUp applies all events needed to catchup up to the subscription.
// If we have multiple replicas for one subscription, we will have only one catch up running.
func catchUp(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository,
	topic string,
	splits int,
	subscriber Consumer,
	projection Projection,
	resumeStore store.KVRStore,
) error {
	if lockerFactory != nil {
		name := fmt.Sprintf("%s:%s#%d-lock", projection.Name(), topic)
		locker := lockerFactory(name)

		var err error
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
	}

	subPos, er := subscriber.RecordPositions(ctx)
	if er != nil {
		return faults.Wrap(er)
	}
	var after eventid.EventID
	// only replay partitions are in the catchup phase
	for part := range subPos {
		// get the sequence for the part
		token, err := getSavedToken(ctx, topic, part, projection, resumeStore)
		if err != nil {
			return faults.Wrap(err)
		}
		// ignore replay of partitions in the consumer phase
		if token.Kind() == ConsumerToken {
			continue
		}

		// after will be the min event ID - if max we would potentially miss events
		if after.IsZero() || after.Compare(token.CatchupEventID()) < 0 {
			after = token.eventID
		}
	}

	var until eventid.EventID
	for _, pos := range subPos {
		// until will be the min event ID - if max we would potentially miss events
		if until.IsZero() || until.Compare(pos.EventID) < 0 {
			until = pos.EventID
		}
	}

	var wg sync.WaitGroup

	errsCh := make(chan error, len(subPos))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for split := 1; split <= splits; split++ {
		split := split
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := catching(ctx, logger, esRepo, after, until, uint32(splits), uint32(split), projection)
			// err := catchUp(ctx, logger, lockerFactory, esRepo, topic, splits, split, subPos, projection, resumeStore)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return
				}
				logger.WithError(err).Error("catching up projection")
				cancel()
				errsCh <- err
				return
			}
		}()
	}

	wg.Wait()

	close(errsCh)
	var err error
	for e := range errsCh {
		err = errors.Join(err, e)
	}
	if err != nil {
		return faults.Errorf("catching for all partitions %v: %w", subPos, err)
	}

	logger.Info("Finished successfully catching up projection")

	err = subscriber.StartConsumer(ctx, projection)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return nil
		}
		return fmt.Errorf("starting consumer")
	}

	return nil
}

func catching(
	ctx context.Context,
	logger log.Logger,
	esRepo EventsRepository,
	startAt eventid.EventID,
	until eventid.EventID,
	partitions, partition uint32,
	projection Projection,
) error {
	logger.WithTags(log.Tags{"startAt": startAt}).Info("Catching up events")
	options := projection.CatchUpOptions()

	// safety margin
	offset := util.IfNil(options.StartOffset, defaultUntilOffset)
	startAt = startAt.OffsetTime(-offset)

	player := NewPlayer(esRepo)

	// loop until it is safe to switch to the subscriber
	lastReplayed := startAt
	logger.WithTags(log.Tags{"from": lastReplayed, "until": until}).Info("Replaying all events from the event store")
	option := store.WithFilter(store.Filter{
		AggregateKinds: options.AggregateKinds,
		Metadata:       options.Metadata,
		Splits:         partitions,
		Split:          partition,
	})
	// first catch up (this can take days)
	lastReplayed, err := player.Replay(ctx, projection.Handle, lastReplayed, until, option)
	if err != nil {
		return faults.Errorf("replaying events from '%d' until '%d': %w", lastReplayed, until, err)
	}

	logger.WithTags(log.Tags{"from": startAt, "until": lastReplayed}).
		Info("All events replayed for the catchup.")

	return nil
}

func getSavedToken(ctx context.Context, topic string, partition uint32, prj Projection, resumeStore store.KVRStore) (Token, error) {
	resume, err := NewResumeKey(prj.Name(), topic, partition)
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	data, err := resumeStore.Get(ctx, resume.String())
	if errors.Is(err, store.ErrResumeTokenNotFound) {
		return NewCatchupToken(eventid.Zero), nil
	}

	token, err := ParseToken(data)
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	return token, faults.Wrap(err)
}
