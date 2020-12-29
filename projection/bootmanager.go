package projection

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/eventid"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/eventstore/worker"
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
	// GetResumeEventIDs gets the smallest of the latest event ID for each partitioned topic from the DB, per aggregate
	GetResumeEventIDs(ctx context.Context) (map[string]string, error)
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
	stages      []BootStage

	cancel        context.CancelFunc
	doneConsumers []chan struct{}
	mu            sync.RWMutex
}

// BootStage represents the different aggregates used to build a projection.
// This aggregates can come from different event stores
type BootStage struct {
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
	stages ...BootStage,
) *ProjectionPartition {
	mc := &ProjectionPartition{
		restartLock: restartLock,
		projection:  projection,
		notifier:    notifier,
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
	for {
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
	prjEventIDs, err := m.projection.GetResumeEventIDs(ctx)
	if err != nil {
		return fmt.Errorf("Could not get last event ID from the projection: %w", err)
	}

	// To avoid the creation of a potential massive buffer size
	// and to ensure that events are not lost, between the switch to the consumer,
	// we execute the fetch in several steps.
	// 1) Process all events from the ES from the begginning - it may be a long operation
	// 2) start the consumer to track new events from now on
	// 3) process any event that may have arrived during the switch
	// 4) start consuming events from the last position
	handler := m.projection.Handler

	lastEventIDs := []string{}
	for _, stage := range m.stages {
		aggregateTypes := stage.AggregateTypes
		// for the aggregates of this stage find the smallest
		prjEventID := common.MaxEventID
		for _, at := range aggregateTypes {
			tmp, ok := prjEventIDs[at]
			if ok && tmp < prjEventID {
				prjEventID = tmp
			}
		}
		// if no event was found in all aggregates, then use the min event id
		if prjEventID == common.MaxEventID {
			prjEventID = common.MinEventID
		}

		// replay oldest events
		prjEventID, err = eventid.DelayEventID(prjEventID, player.TrailingLag)
		if err != nil {
			return fmt.Errorf("Error delaying the eventID: %w", err)
		}
		log.Printf("Booting %s from '%s'", m.projection.GetName(), prjEventID)

		replayer := player.New(stage.Repository)
		filter := store.WithAggregateTypes(aggregateTypes...)
		lastEventID, err := replayer.Replay(ctx, handler, prjEventID, filter)
		if err != nil {
			return fmt.Errorf("Could not replay all events (first part): %w", err)
		}
		lastEventIDs = append(lastEventIDs, lastEventID)
	}

	frozen := make([]chan struct{}, 0)
	for k, stage := range m.stages {
		// grab the last events sequences in the topic (partitioned)
		partitionSize := stage.PartitionHi - stage.PartitionLo + 1
		tokens := make([]string, partitionSize)
		for i := uint32(0); i < partitionSize; i++ {
			tokens[i], err = stage.Subscriber.GetResumeToken(ctx, stage.PartitionLo+i)
			if err != nil {
				return fmt.Errorf("Could not retrieve resume token: %w", err)
			}
		}

		// consume potential missed events events between the switch to the consumer
		events, err := stage.Repository.GetEvents(ctx, lastEventIDs[k], 0, time.Duration(0), store.Filter{
			AggregateTypes: stage.AggregateTypes,
		})
		if err != nil {
			return fmt.Errorf("Could not replay all events (second part): %w", err)
		}
		for _, event := range events {
			err = handler(ctx, event)
			if err != nil {
				return fmt.Errorf("Error handling event %+v: %w", event, err)
			}
		}

		// start consuming events from the last available position
		for i := uint32(0); i < partitionSize; i++ {
			ch, err := stage.Subscriber.StartConsumer(ctx, stage.PartitionLo+i, tokens[i], m.projection, stage.AggregateTypes)
			if err != nil {
				return fmt.Errorf("Unable to start consumer: %w", err)
			}
			frozen = append(frozen, ch)
		}
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
