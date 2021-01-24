package projection

import (
	"context"
	"sync"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/eventid"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/eventstore/worker"
	"github.com/quintans/faults"
	log "github.com/sirupsen/logrus"
)

// Canceller is the interface for cancelling a running projection
type Canceller interface {
	Name() string
	Cancel()
}

// Projection is the interface that a projection needs to implement
type Projection interface {
	GetName() string
	// GetResumeEventIDs gets the smallest of the latest event ID for a partition topic from the DB, for the aggregate group
	GetResumeEventIDs(ctx context.Context, aggregateTypes []string, partition uint32) (string, error)
	Handler(ctx context.Context, e eventstore.Event) error
}

// Notifier represents the interface that a message queue needs to implement to able to cancel distributed projections execution
type Notifier interface {
	ListenCancelProjection(ctx context.Context, restarter Canceller) error
	CancelProjection(ctx context.Context, projectionName string, partitions int) error
}

type Subscriber interface {
	StartConsumer(ctx context.Context, partition uint32, resumeToken string, projection Projection, aggregateTypes []string) (chan struct{}, error)
	GetResumeToken(ctx context.Context, partition uint32) (string, error)
}

type EventHandler func(ctx context.Context, e eventstore.Event) error

type ProjectionPartition struct {
	restartLock worker.WaitForUnlocker
	projection  Projection
	notifier    Notifier
	partitions  uint32
	stages      []BootStage

	cancel        context.CancelFunc
	doneConsumers []chan struct{}
	mu            sync.RWMutex
}

// BootStage represents the different aggregates used to build a projection.
// This aggregates can come from different event stores
type BootStage struct {
	// AggregateTypes is the list of aggregates on the same topic
	AggregateTypes []string
	Subscriber     Subscriber
	// Repository: Repository to the events
	Repository player.Repository
	// PartitionLo: low partition number. if zero, partitioning will ignored
	PartitionLo uint32
	// PartitionHi: high partition number. if zero, partitioning will ignored
	PartitionHi uint32
}

// NewProjectionPartition creates an instance that manages the lifecycle of a projection that has the capability of being stopped and restarted on demand.
// Arguments:
// @param projection: the projection
// @param notifier: handles all interaction with freezing/unfreezing notifications
// @param stages: booting can be done in stages, since different event stores can be involved
func NewProjectionPartition(
	restartLock worker.WaitForUnlocker,
	projection Projection,
	notifier Notifier,
	partitions uint32,
	stages ...BootStage,
) *ProjectionPartition {
	mc := &ProjectionPartition{
		restartLock: restartLock,
		projection:  projection,
		notifier:    notifier,
		partitions:  partitions,
		stages:      stages,
	}

	return mc
}

// Name returns the name of this projection
func (m *ProjectionPartition) Name() string {
	return m.projection.GetName()
}

// Run action to be executed on boot
func (m *ProjectionPartition) Run(ctx context.Context) error {
	logger := log.WithField("projection", m.projection.GetName())
	for {
		logger.Info("Waiting for Unlock")
		m.restartLock.WaitForUnlock(ctx)

		ctx2, cancel := context.WithCancel(ctx)
		m.mu.Lock()
		m.cancel = cancel
		m.mu.Unlock()

		err := m.bootAndListen(ctx2)
		if err != nil {
			cancel()
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (m *ProjectionPartition) bootAndListen(ctx context.Context) error {
	err := m.boot(ctx)
	if err != nil {
		return err
	}

	err = m.notifier.ListenCancelProjection(ctx, m)
	if err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

func (m *ProjectionPartition) boot(ctx context.Context) error {
	logger := log.WithField("projection", m.projection.GetName())

	handler := m.projection.Handler

	frozen := make([]chan struct{}, 0)
	for k, stage := range m.stages {
		// To avoid the creation of a potential massive buffer size
		// and to ensure that events are not lost, between the switch to the consumer,
		// we execute the fetch in several steps.
		// EACH PARTITION CAN BE INDEPENDENT OF EACH OTHER AND CAN BE TREATED AS IF IT IS THE ONLY PARTITION
		// 1) Process all events from the ES from the begginning - it may be a long operation
		// 2) start the consumer to track new events from now on
		// 3) process any event that may have arrived during the switch
		// 4) start consuming events from the last position

		prjEventIDs := map[uint32]string{}
		smallestEventID := string([]byte{255})
		for partition := stage.PartitionLo; partition <= stage.PartitionHi; partition++ {
			// for the aggregate group in the same partition (=same source), gets the latest event ID.
			prjEventID, err := m.projection.GetResumeEventIDs(ctx, stage.AggregateTypes, partition)
			if err != nil {
				return faults.Errorf("Could not get last event ID from the projection: %w", err)
			}

			// to make sure we don't miss any event due to clock skews, we start replaying a bit earlier
			prjEventID, err = eventid.DelayEventID(prjEventID, player.TrailingLag)
			if err != nil {
				return faults.Errorf("Error delaying the eventID: %w", err)
			}

			if smallestEventID > prjEventID {
				smallestEventID = prjEventID
			}

			prjEventIDs[partition] = prjEventID
		}

		logger.Infof("Beginning booting stage %d from event ID '%s'", k, smallestEventID)

		// replaying events for each partition, rather than replay from the smallest event of the partition range,
		// probably is faster because the projection does not have to handle repeated events.

		// will only handle events that have not been handled for a given partition
		handlerFilter := func(event eventstore.Event) bool {
			p := common.WhichPartition(event.AggregateIDHash, m.partitions)
			eID := prjEventIDs[p]

			return event.AggregateID > eID
		}
		replayer := player.New(stage.Repository, player.WithCustomFilter(handlerFilter))
		lastEventID, err := replayer.Replay(ctx, handler, smallestEventID,
			store.WithAggregateTypes(stage.AggregateTypes...),
			store.WithPartitions(m.partitions, stage.PartitionLo, stage.PartitionHi),
		)
		if err != nil {
			return faults.Errorf("Could not replay all events (first part): %w", err)
		}

		for partition := stage.PartitionLo; partition <= stage.PartitionHi; partition++ {
			// grab the last events sequences in the partition
			token, err := stage.Subscriber.GetResumeToken(ctx, partition)
			if err != nil {
				return faults.Errorf("Could not retrieve resume token for projection %s and partition %d: %w", m.projection.GetName(), partition, err)
			}

			p := player.New(stage.Repository,
				player.WithBatchSize(0),
				player.WithTrailingLag(time.Duration(0)),
			)
			_, err = p.Replay(ctx, handler, lastEventID,
				store.WithAggregateTypes(stage.AggregateTypes...),
				store.WithPartitions(m.partitions, stage.PartitionLo, stage.PartitionHi),
			)
			if err != nil {
				return faults.Errorf("Could not replay all events for projection %s (second part): %w", m.projection.GetName(), err)
			}

			// start consuming events from the last available position
			ch, err := stage.Subscriber.StartConsumer(ctx, partition, token, m.projection, stage.AggregateTypes)
			if err != nil {
				return faults.Errorf("Unable to start consumer projection %s: %w", m.projection.GetName(), err)
			}
			frozen = append(frozen, ch)
		}

		logger.Infof("Ended booting stage %d", k)
	}

	m.mu.Lock()
	m.doneConsumers = frozen
	m.mu.Unlock()

	return nil
}

func (m *ProjectionPartition) Cancel() {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	// wait for the closing subscriber
	for _, ch := range m.doneConsumers {
		<-ch
	}

	m.mu.Unlock()
}
