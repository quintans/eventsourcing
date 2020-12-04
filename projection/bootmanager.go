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
	log "github.com/sirupsen/logrus"
)

type Freezer interface {
	Name() string
	Freeze() bool
	Unfreeze()
}

type Projection interface {
	GetName() string
	// GetResumeEventIDs gets the smallest of the latest event ID for each partitioned topic from the DB, per aggregate
	GetResumeEventIDs(ctx context.Context) (map[string]string, error)
	Handler(ctx context.Context, e eventstore.Event) error
}

type Notifier interface {
	ListenForNotifications(ctx context.Context, freezer Freezer) error
	FreezeProjection(ctx context.Context, projectionName string) error
	UnfreezeProjection(ctx context.Context, projectionName string) error
}

type Subscriber interface {
	StartConsumer(ctx context.Context, partition uint32, resumeToken string, projection Projection, aggregateTypes []string) (chan struct{}, error)
	GetResumeToken(ctx context.Context, partition uint32) (string, error)
}

type EventHandler func(ctx context.Context, e eventstore.Event) error

type BootableManager struct {
	projection Projection
	notifier   Notifier
	stages     []BootStage

	cancel  context.CancelFunc
	wait    chan struct{}
	release chan struct{}
	closed  bool
	frozen  []chan struct{}
	hasLock bool // acquired the lock
	mu      sync.RWMutex
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

// NewBootableManager creates an instance that manages the lifecycle of a projection that has the capability of being stopped and restarted on demand.
// Arguments:
// @param projection: the projection
// @param notifier: handles all interaction with freezing/unfreezing notifications
// @param stages: booting can be done in stages, since different event stores can be involved
func NewBootableManager(
	projection Projection,
	notifier Notifier,
	stages ...BootStage,
) *BootableManager {
	c := make(chan struct{})
	close(c)
	mc := &BootableManager{
		projection: projection,
		notifier:   notifier,
		stages:     stages,
		wait:       c,
		release:    make(chan struct{}),
	}

	return mc
}

func (m *BootableManager) Name() string {
	return m.projection.GetName()
}

// Wait block the call if the MonitorableConsumer was put on hold
func (m *BootableManager) Wait() <-chan struct{} {
	m.mu.Lock()
	wait := m.wait
	m.mu.Unlock()

	<-wait

	m.mu.Lock()
	defer m.mu.Unlock()
	m.release = make(chan struct{})
	return m.release
}

// OnBoot action to be executed on boot
func (m *BootableManager) OnBoot(ctx context.Context) error {
	var ctx2 context.Context
	ctx2, m.cancel = context.WithCancel(ctx)
	err := m.boot(ctx2)
	if err != nil {
		m.cancel()
		return err
	}

	err = m.notifier.ListenForNotifications(ctx, m)
	if err != nil {
		m.cancel()
		return err
	}

	m.hasLock = true
	return nil
}

func (m *BootableManager) boot(ctx context.Context) error {
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
			tmp := prjEventIDs[at]
			if tmp < prjEventID {
				prjEventID = tmp
			}
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
	m.frozen = frozen
	m.mu.Unlock()

	return nil
}

func (m *BootableManager) Cancel() {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
}

// Freeze blocks calls to Wait() return true if this instance was locked
func (m *BootableManager) Freeze() bool {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.wait = make(chan struct{})
	m.closed = false
	close(m.release)
	m.release = make(chan struct{})

	if m.hasLock {
		// wait for the closing subscriber
		for _, ch := range m.frozen {
			<-ch
		}
	}
	locked := m.hasLock
	m.hasLock = false

	m.mu.Unlock()

	return locked
}

// Unfreeze releases any blocking call to Wait()
// points the cursor to the first available record
func (m *BootableManager) Unfreeze() {
	m.mu.Lock()
	if !m.closed {
		close(m.wait)
		m.closed = true
	}
	m.mu.Unlock()
}

type Action int

const (
	Freeze Action = iota + 1
	Unfreeze
)

type Notification struct {
	Projection string `json:"projection"`
	Action
}
