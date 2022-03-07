package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/worker"
)

type (
	WaitLockerFactory func(string) lock.WaitLocker
)

type ProjectionHandler struct {
	topic      string
	partitions uint32
	handler    projection.EventHandlerFunc
}

type RestartableProjector struct {
	logger            log.Logger
	nc                *nats.Conn
	stream            nats.JetStreamContext
	waitLockerFactory WaitLockerFactory
	membersCount      int
	resumeStore       projection.ResumeStore
	projectionName    string
	handlers          []ProjectionHandler
	startStop         *projection.RestartableProjection
}

func NewRestartableProjector(
	ctx context.Context,
	logger log.Logger,
	url string,
	lockerFactory WaitLockerFactory,
	membersCount int,
	resumeStore projection.ResumeStore,
	projectionName string,
) (*RestartableProjector, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	p, err := NewRestartableProjectorWithConn(ctx, logger, nc, lockerFactory, membersCount, resumeStore, projectionName)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		p.Shutdown(false)
		nc.Close()
	}()

	return p, nil
}

func NewRestartableProjectorWithConn(
	ctx context.Context,
	logger log.Logger,
	nc *nats.Conn,
	lockerFactory WaitLockerFactory,
	membersCount int,
	resumeStore projection.ResumeStore,
	projectionName string,
) (*RestartableProjector, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return &RestartableProjector{
		nc:                nc,
		stream:            stream,
		logger:            logger,
		waitLockerFactory: lockerFactory,
		membersCount:      membersCount,
		projectionName:    projectionName,
		resumeStore:       resumeStore,
	}, nil
}

func (p *RestartableProjector) AddTopicHandler(topic string, partitions uint32, handler projection.EventHandlerFunc) {
	p.handlers = append(p.handlers, ProjectionHandler{
		topic:      topic,
		partitions: partitions,
		handler:    handler,
	})
}

func (p *RestartableProjector) Projection(ctx context.Context) (*projection.NotifierLockRebuilder, *projection.RestartableProjection, error) {
	if len(p.handlers) == 0 {
		return nil, nil, faults.Errorf("no handlers defined for projector %s", p.projectionName)
	}
	var subscribers []projection.Subscriber
	consumerFactory := func(ctx context.Context, resume projection.ResumeKey) (projection.Consumer, error) {
		sub := NewSubscriber(p.logger, p.stream, p.resumeStore, resume)
		subscribers = append(subscribers, sub)
		return sub, nil
	}

	var workers []worker.Worker
	for _, h := range p.handlers {
		// create workers according to the topic that we want to listen
		w, err := projection.UnmanagedWorkers(ctx, p.logger, p.projectionName, h.topic, h.partitions, consumerFactory, h.handler)
		if err != nil {
			return nil, nil, faults.Wrap(err)
		}
		workers = append(workers, w...)
	}

	natsProjectionCanceller, err := NewNotificationListenerWithConn(ctx, p.logger, p.nc, p.projectionName+"_notifications")
	if err != nil {
		return nil, nil, faults.Errorf("Error creating NATS canceller subscriber: %w", err)
	}
	locker := p.waitLockerFactory(p.projectionName + "-freeze")

	p.startStop = projection.NewRestartableProjection(p.logger, p.projectionName, locker, natsProjectionCanceller, workers)

	rebuilder := projection.NewNotifierLockRestarter(
		p.logger,
		locker,
		natsProjectionCanceller,
		subscribers,
		p.membersCount,
	)

	return rebuilder, p.startStop, nil
}

func (p *RestartableProjector) Shutdown(hard bool) {
	p.startStop.Cancel(context.Background(), hard)
}

// NewReactor creates workers that listen to events coming through the event bus,
// forwarding them to an handler. This is the same approache used for projections
// but where we don't care about replays.
func NewReactor(
	ctx context.Context,
	logger log.Logger,
	url string,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (<-chan struct{}, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, faults.Errorf("Could not instantiate Nats jetstream context: %w", err)
	}

	reactDone, err := NewReactorWithConn(ctx, logger, js, projectionName, topic, partitions, handler)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		nc.Close()
		<-reactDone
		close(done)
	}()

	return done, nil
}

func NewReactorWithConn(
	ctx context.Context,
	logger log.Logger,
	stream nats.JetStreamContext,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (<-chan struct{}, error) {
	consumerFactory := func(_ context.Context, resumeKey projection.ResumeKey) (projection.Consumer, error) {
		return NewReactorSubscriber(logger, stream, resumeKey), nil
	}

	workers, err := projection.UnmanagedWorkers(ctx, logger, projectionName, topic, partitions, consumerFactory, handler)
	if err != nil {
		return nil, faults.Errorf("Error creating unmanaged workers: %w", err)
	}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		for _, w := range workers {
			w.Stop(context.Background())
		}
		close(done)
	}()

	return done, nil
}