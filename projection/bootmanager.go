package projection

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/player"
)

type Projection interface {
	Subscriber
	GetResumeEventID(ctx context.Context) (string, error)
}

type Subscriber interface {
	GetResumeToken(ctx context.Context, topic string, partition int) (string, error)
	Consume(ctx context.Context, topic string, partition int, resumeToken string, aggregateTypes []string, handler EventHandler) error
	Notifier(ctx context.Context, managerTopic string, bootManager *BootableManager) error
}

type EventHandler func(ctx context.Context, e eventstore.Event) error

type BootableManager struct {
	name           string
	projection     Projection
	aggregateTypes []string
	esRepository   player.Repository
	topic          string
	partitionLo    int
	partitionHi    int
	managerTopic   string
	handler        EventHandler

	cancel  context.CancelFunc
	wait    chan struct{}
	release chan struct{}
	closed  bool
	locked  bool // has the dist lock
	mu      sync.RWMutex
}

func NewBootableManager(
	name string,
	projection Projection,
	aggregateTypes []string,
	esRepository player.Repository,
	topic string,
	partitionLo,
	partitionHi int,
	managerTopic string,
	handler EventHandler,
) *BootableManager {
	c := make(chan struct{})
	close(c)
	mc := &BootableManager{
		name:           name,
		projection:     projection,
		aggregateTypes: aggregateTypes,
		esRepository:   esRepository,
		partitionLo:    partitionLo,
		partitionHi:    partitionHi,
		managerTopic:   managerTopic,
		handler:        handler,
		wait:           c,
		release:        make(chan struct{}),
	}
	return mc
}

func (m *BootableManager) Name() string {
	return m.name
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
	ctx, m.cancel = context.WithCancel(ctx)
	err := m.boot(ctx)
	if err != nil {
		return err
	}
	m.locked = true
	return nil
}

func (m *BootableManager) boot(ctx context.Context) error {
	// get the latest event ID from the DB
	prjEventID, err := m.projection.GetResumeEventID(ctx)
	if err != nil {
		return fmt.Errorf("Could not get last event ID from the projection: %w", err)
	}

	// To avoid the creation of  potential massive buffer size
	// and to ensure that events are not lost, between the switch to the consumer,
	// we execute the fetch in several steps.
	// 1) Process all events from the ES from the begginning
	// 2) start the consumer to track new events from now on
	// 3) process any event that may have arrived between the switch
	// 4) start consuming events from the last position
	replayer := player.New(m.esRepository, 20)
	handler := func(ctx context.Context, e eventstore.Event) error {
		err := m.handler(ctx, e)
		if err != nil {
			return err
		}
		return err
	}
	// replay oldest events
	filter := player.WithAggregateTypes(m.aggregateTypes...)
	lastEventID, err := replayer.ReplayFromUntil(ctx, handler, prjEventID, "", filter)
	if err != nil {
		return fmt.Errorf("Could not replay all events (first part): %w", err)
	}

	// grab the last events sequences in the topic (partitioned)
	partitionSize := m.partitionHi - m.partitionLo + 1
	tokens := make([]string, partitionSize)
	for i := 0; i < partitionSize; i++ {
		tokens[i], err = m.projection.GetResumeToken(ctx, m.topic, m.partitionHi+i)
		if err != nil {
			return fmt.Errorf("Could not retrieve resume token: %w", err)
		}
	}

	// consume potential missed events events between the switch to the consumer
	lastEventID, err = replayer.ReplayFromUntil(ctx, handler, lastEventID, "", filter)
	if err != nil {
		return fmt.Errorf("Could not replay all events (second part): %w", err)
	}

	// start consuming events from the last available position
	for i := 0; i < partitionSize; i++ {
		partition := m.partitionHi + i
		err := m.projection.Consume(ctx, m.topic, partition, tokens[i], m.aggregateTypes, m.handler)
		return fmt.Errorf("Unable to start consumer: %w", err)
	}

	return m.projection.Notifier(ctx, m.managerTopic, m)
}

// IsLocked return if this instance is locked
func (m *BootableManager) IsLocked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.locked
}

// Freeze blocks calls to Wait()
func (m *BootableManager) Freeze() {
	m.mu.Lock()
	m.cancel()
	m.wait = make(chan struct{})
	m.closed = false
	close(m.release)
	m.release = make(chan struct{})
	m.locked = false
	m.mu.Unlock()
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
