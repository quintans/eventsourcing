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
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

const (
	defaultUntilOffset   = 15 * time.Minute
	defaultCatchupWindow = 3 * 24 * time.Hour // 3 days
	checkPointBuffer     = 1_000
)

type resumeKV struct {
	eventID   eventid.EventID
	partition uint32
	sequence  uint64
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
	logger log.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository,
	subscriber Consumer,
	projection Projection,
	splits int,
	resumeStore store.KVStore,
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
			// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
			checkPointCh := make(chan resumeKV, checkPointBuffer)
			go func() {
				for kv := range checkPointCh {
					if (kv == resumeKV{}) {
						// quit received
						return
					}

					var t Token
					if !kv.eventID.IsZero() {
						t = NewCatchupToken(kv.eventID)
					} else {
						t = NewConsumerToken(kv.sequence)
					}
					err := saveResume(ctx, resumeStore, projection.Name(), topic, kv.partition, t)
					if err != nil {
						logger.WithError(err).WithTags(log.Tags{
							"resumeKey": t.String(),
							"token":     t.String(),
						}).Errorf("Failed to save resume token")
					}
				}
			}()
			go func() {
				<-ctx.Done()
				fmt.Println("===> quitting projection worker")
				checkPointCh <- resumeKV{} // signal quit
			}()

			err := catchUp(ctx, logger, lockerFactory, esRepo, topic, splits, joinedParts, subscriber, projection, resumeStore, checkPointCh)
			if err != nil {
				return faults.Wrap(err)
			}

			handler := func(ctx context.Context, e *sink.Message, partition uint32, seq uint64) error {
				fmt.Printf("===> consuming event: %+v (partition=%d, sequence=%d)\n", e, partition, seq)
				er := projection.Handle(ctx, e)
				if er != nil {
					return faults.Wrap(er)
				}

				checkPointCh <- resumeKV{sequence: seq, partition: partition}
				return nil
			}

			logger.Info("Starting consumer for projection")
			err = subscriber.StartConsumer(ctx, projection.Name(), handler)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				return faults.Errorf("starting consumer: %w", err)
			}

			return nil
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
	joinedParts string,
	subscriber Consumer,
	projection Projection,
	resumeStore store.KVStore,
	checkPointCh chan resumeKV,
) error {
	if lockerFactory != nil {
		name := fmt.Sprintf("%s:%s#%s-lock", projection.Name(), topic, joinedParts)
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

	var subPos map[uint32]SubscriberPosition
	options := projection.CatchUpOptions()
	catchUpWindow := util.IfZero(options.CatchUpWindow, defaultCatchupWindow)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		start := time.Now()

		var er error
		subPos, er = subscriber.Positions(ctx)
		if er != nil {
			return faults.Wrap(er)
		}

		if len(subPos) == 0 {
			return faults.New("no subscriber positions were returned")
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

		cmp := after.Compare(until)
		if cmp == 0 {
			logger.Info("There is nothing to catchup")
			// there is nothing to catchup
			return nil
		}
		if cmp > 0 {
			// we are in an inconsistent state, so we error
			return faults.Errorf(
				"the events bus (%s) is behind the projection (%s-%s-%s=%s) witch is a problem",
				after, projection.Name(), topic, joinedParts, until,
			)
		}

		var wg sync.WaitGroup

		errsCh := make(chan error, len(subPos))

		for split := 1; split <= splits; split++ {
			split := split
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := catching(ctx, logger, esRepo, after, until, uint32(splits), uint32(split), projection, checkPointCh)
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

		// if the catch up took less than catchUpWindow we can safely exit and switch to the event bus
		if time.Since(start) < catchUpWindow {
			break
		}
	}

	// save all positions before switching
	err := saveConsumerPositions(ctx, resumeStore, projection.Name(), topic, subPos)
	if err != nil {
		return faults.Wrap(err)
	}

	logger.Info("Finished successfully catching up projection")

	return nil
}

func catching(
	ctx context.Context,
	logger log.Logger,
	esRepo EventsRepository,
	after eventid.EventID,
	until eventid.EventID,
	partitions, partition uint32,
	projection Projection,
	checkPointCh chan resumeKV,
) error {
	logger.WithTags(log.Tags{"startAt": after}).Info("Catching up events")
	options := projection.CatchUpOptions()

	// safety margin
	offset := util.IfZero(options.StartOffset, defaultUntilOffset)
	after = after.OffsetTime(-offset)

	player := NewPlayer(esRepo)

	// loop until it is safe to switch to the subscriber
	lastReplayed := after

	logger.WithTags(log.Tags{"from": lastReplayed, "until": until}).Info("Replaying all events from the event store")
	option := store.WithFilter(store.Filter{
		AggregateKinds: options.AggregateKinds,
		Metadata:       options.Metadata,
		Splits:         partitions,
		Split:          partition,
	})

	handle := func(ctx context.Context, msg *sink.Message) error {
		err := projection.Handle(ctx, msg)
		if err != nil {
			return faults.Wrap(err)
		}

		checkPointCh <- resumeKV{eventID: msg.ID, partition: partition}

		return nil
	}

	var err error
	lastReplayed, err = player.Replay(ctx, handle, lastReplayed, until, option)
	if err != nil {
		return faults.Errorf("replaying events from '%d' until '%d': %w", lastReplayed, until, err)
	}

	logger.WithTags(log.Tags{"from": after, "until": lastReplayed}).
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

func saveConsumerPositions(ctx context.Context, resumeStore store.KVStore, prjName, topic string, subPos map[uint32]SubscriberPosition) (e error) {
	defer faults.Catch(&e, "saveConsumerPositions")

	for partition, pos := range subPos {
		err := saveResume(ctx, resumeStore, prjName, topic, partition, NewConsumerToken(pos.Position))
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}

func saveResume(ctx context.Context, resumeStore store.KVStore, prjName, topic string, partition uint32, t Token) (e error) {
	defer faults.Catch(&e, "saveResume(prjName=%s, topic=%s, partition=%d)", prjName, topic, partition)

	resume, err := NewResumeKey(prjName, topic, partition)
	if err != nil {
		return faults.Wrap(err)
	}

	return resumeStore.Put(ctx, resume.String(), t.String())
}
