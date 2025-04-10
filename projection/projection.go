package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

const (
	defaultUntilOffset          = time.Minute
	defaultCatchupWindow        = 3 * 24 * time.Hour // 3 days
	checkPointBuffer            = 1_000
	defaultUpdateResumeInterval = time.Second
)

type resumeKV struct {
	done      bool
	eventID   eventid.EventID
	partition uint32
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
func Project[K eventsourcing.ID](
	logger *slog.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository[K],
	subscriber Consumer[K],
	projection Projection[K],
	resumeStore store.KVStore,
	catchupSplits int,
) *worker.RunWorker {
	topic := subscriber.Topic()
	workerName := fmt.Sprintf("%s-%s", projection.Name(), topic)
	logger = logger.With(
		"projection", projection.Name(),
	)

	return worker.NewRun(
		logger,
		workerName,
		projection.Name(),
		nil,
		func(ctx context.Context) error {
			// we use the partitions from the subscriber as a convenient way of distributing the catchup
			err := resume(ctx, logger, lockerFactory, esRepo, topic, catchupSplits, subscriber, projection, resumeStore)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return nil
				}
				return faults.Wrap(err)
			}
			return nil
		},
	)
}

func asyncSaveResumes(ctx context.Context, logger *slog.Logger, resumeStore store.KVStore, projName, topic string) (chan<- resumeKV, <-chan struct{}) {
	checkPointCh := make(chan resumeKV, checkPointBuffer)
	done := make(chan struct{})

	// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
	go func() {
		defer close(done)
		for kv := range checkPointCh {
			if (kv == resumeKV{}) {
				// quit received
				return
			}

			t := Token{Done: kv.done, EventID: kv.eventID}
			err := saveResume(ctx, resumeStore, projName, topic, kv.partition, t)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return
				}
				logger.Error("Failed to save resume token",
					"prjName", projName, "topic", topic, "partition", kv.partition,
					"token", t.String(),
					log.Err(err),
				)
			}
		}
	}()
	go func() {
		<-ctx.Done()
		checkPointCh <- resumeKV{} // signal quit
	}()

	return checkPointCh, done
}

// resume first applies all events needed to catchup up to the subscription and then start consuming from the stream.
//
// If we have multiple replicas for one subscription, we will have only one catch up running.
func resume[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	lockerFactory LockerFactory,
	esRepo EventsRepository[K],
	topic string,
	catchupSplits int,
	subscriber Consumer[K],
	projection Projection[K],
	resumeStore store.KVStore,
) error {
	if lockerFactory != nil {
		name := fmt.Sprintf("%s:%s-lock", projection.Name(), topic)
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
					logger.Error("unlock on catchUp", log.Err(er))
				}
			}()
		}
	}

	checkPointCh, done := asyncSaveResumes(ctx, logger, resumeStore, projection.Name(), topic)

	options := projection.CatchUpOptions()
	catchUpWindow := util.IfZero(options.CatchUpWindow, defaultCatchupWindow)

	catchupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	name := fmt.Sprintf("%s:%s", projection.Name(), topic)
	for {
		start := time.Now()

		after, catchup, er := catchupAfter[K](catchupCtx, topic, projection, resumeStore, uint32(catchupSplits))
		if er != nil {
			return faults.Wrap(er)
		}

		if !catchup && start.Sub(after.Time()) < catchUpWindow {
			logger.Info("There is nothing to catchup. Starting consumer from the last position")
			return startConsuming(ctx, name, nil, subscriber, projection)
		}

		var wg sync.WaitGroup

		errsCh := make(chan error, catchupSplits)

		offset := util.IfZero(options.StartOffset, defaultUntilOffset)
		until := eventid.NewWithTime(start).OffsetTime(-offset)

		if after.Compare(until) < 0 {
			for split := 1; split <= catchupSplits; split++ {
				split := split
				wg.Add(1)

				go func() {
					defer wg.Done()

					err := catching[K](catchupCtx, logger, name, esRepo, after, until, uint32(catchupSplits), uint32(split), projection, checkPointCh)
					if err != nil {
						if errors.Is(err, catchupCtx.Err()) {
							return
						}
						logger.Error("catching up projection", log.Err(err))
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
				return faults.Errorf("catching for all splits %d: %w", catchupSplits, err)
			}
		}

		// if the catch up took less than catchUpWindow we can safely exit and switch to the event bus
		if time.Since(start) < catchUpWindow {
			checkPointCh <- resumeKV{} // signal quit

			// waits for all the catchup resume tokens to be saved
			<-done

			err := projection.Handle(ctx, Message[K]{
				Meta: Meta{
					Name: name,
					Kind: MessageKindSwitch,
				},
				Message: &sink.Message[K]{
					ID: until,
				},
			})
			if err != nil {
				return faults.Errorf("signalling catchup to live switch: %w", err)
			}

			startTime := until.Time().Add(-offset)
			err = startConsuming(ctx, name, &startTime, subscriber, projection)
			if err != nil {
				return faults.Errorf("starting to consume after catch up: %w", err)
			}

			logger.Info("Marking all catchup resumes as done")
			for split := 1; split <= catchupSplits; split++ {
				t := Token{Done: true, EventID: until}
				err := saveResume(ctx, resumeStore, projection.Name(), topic, uint32(split), t)
				if err != nil {
					if errors.Is(err, ctx.Err()) {
						break
					}
					logger.Error("Failed to save resume token",
						"projectionName", projection.Name(), "topic", topic, "partition", uint32(split),
						"token", t.String(),
						log.Err(err),
					)
				}
			}

			return nil
		}
	}
}

func startConsuming[K eventsourcing.ID](
	ctx context.Context,
	name string,
	startTime *time.Time,
	subscriber Consumer[K],
	projection Projection[K],
) error {
	handler := func(ctx context.Context, msg *sink.Message[K], partition uint32, seq uint64) error {
		er := projection.Handle(ctx, Message[K]{
			Meta: Meta{
				Name:      name,
				Kind:      MessageKindLive,
				Partition: partition,
				Sequence:  seq,
			},
			Message: msg,
		})
		if er != nil {
			return faults.Wrap(er)
		}

		return nil
	}
	err := subscriber.StartConsumer(ctx, startTime, projection.Name(), handler)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return nil
		}
		return faults.Errorf("starting consumer: %w", err)
	}

	return nil
}

// catchupAfter returns the minimum recorded event ID, the minimum last published event ID and if there is any catchup to be done
func catchupAfter[K eventsourcing.ID](ctx context.Context, topic string, projection Projection[K], resumeStore store.KVStore, catchupSplits uint32) (eventid.EventID, bool, error) {
	var after eventid.EventID
	var catchup bool
	// only replay partitions are in the catchup phase - if there is a crash while switching to consuming
	for i := uint32(1); i <= catchupSplits; i++ {
		// get the sequence for the part
		token, err := getSavedToken(ctx, topic, i, projection.Name(), resumeStore)
		if err != nil {
			return eventid.Zero, false, faults.Wrap(err)
		}
		// ignore replay of partitions in the consumer phase
		if (token != Token{}) && !token.Done {
			catchup = true
		}

		// after will be the min event ID - if max we would potentially miss events
		if after.IsZero() || after.Compare(token.EventID) < 0 {
			after = token.EventID
		}
	}

	return after, catchup, nil
}

func catching[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	name string,
	esRepo EventsRepository[K],
	after eventid.EventID,
	until eventid.EventID,
	partitions, partition uint32,
	projection Projection[K],
	checkPointCh chan<- resumeKV,
) error {
	logger.Info("Catching up events", "startAt", after)
	options := projection.CatchUpOptions()

	playOptions := []Option[K]{}
	if options.BatchSize > 0 {
		playOptions = append(playOptions, WithBatchSize[K](options.BatchSize))
	}

	player := NewPlayer(esRepo, playOptions...)

	logger.Info("Replaying all events from the event store", "from", after, "until", until, "time", until.Time())
	option := store.WithFilter(store.Filter{
		AggregateKinds: options.AggregateKinds,
		Discriminator:  options.Discriminator,
		Splits:         partitions,
		SplitIDs:       []uint32{partition - 1},
	})

	var lastTime sync.Map

	updateResumeInterval := util.IfZero(options.UpdateResumeInterval, defaultUpdateResumeInterval)

	handle := func(ctx context.Context, msg *sink.Message[K]) error {
		err := projection.Handle(ctx, Message[K]{
			Meta: Meta{
				Name:      name,
				Kind:      MessageKindCatchup,
				Partition: partition,
			},
			Message: msg,
		})
		if err != nil {
			return faults.Wrap(err)
		}

		v, ok := lastTime.Load(partition)
		if !ok || time.Since(v.(time.Time)) >= updateResumeInterval {
			checkPointCh <- resumeKV{eventID: msg.ID, partition: partition}
			lastTime.Store(partition, time.Now())
		}

		return nil
	}

	var err error
	_, count, err := player.Replay(ctx, handle, after, until, option)
	if err != nil {
		return faults.Errorf("replaying events from '%d' until '%d': %w", after, until, err)
	}

	logger.Info("All events replayed for the catchup.", "count", count)

	return nil
}

func getSavedToken(ctx context.Context, topic string, partition uint32, prjName string, resumeStore store.KVRStore) (Token, error) {
	resume, err := NewResumeKey(prjName, topic, partition)
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	data, err := resumeStore.Get(ctx, resume.String())
	if errors.Is(err, store.ErrResumeTokenNotFound) {
		return Token{}, nil
	}

	token, err := ParseToken(data)
	if err != nil {
		return Token{}, faults.Wrap(err)
	}

	return token, faults.Wrap(err)
}

func saveResume(ctx context.Context, resumeStore store.KVStore, prjName, topic string, partition uint32, t Token) (e error) {
	defer faults.Catch(&e, "saveResume(prjName=%s, topic=%s, partition=%d)", prjName, topic, partition)

	resume, err := NewResumeKey(prjName, topic, partition)
	if err != nil {
		return faults.Wrap(err)
	}

	return resumeStore.Put(ctx, resume.String(), t.String())
}
