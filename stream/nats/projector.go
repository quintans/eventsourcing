package nats

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/worker"
)

type Projector struct {
	logger         log.Logger
	nc             *nats.Conn
	stream         nats.JetStreamContext
	resumeStore    projection.ResumeStore
	projectionName string
	handlers       []ProjectionHandler
	workers        []worker.Worker
}

func NewProjector(
	ctx context.Context,
	logger log.Logger,
	url string,
	resumeStore projection.ResumeStore,
	projectionName string,
) (*Projector, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	p, err := NewProjectorWithConn(ctx, logger, nc, resumeStore, projectionName)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		nc.Close()
	}()

	return p, nil
}

func NewProjectorWithConn(
	ctx context.Context,
	logger log.Logger,
	nc *nats.Conn,
	resumeStore projection.ResumeStore,
	projectionName string,
) (*Projector, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	logger = logger.WithTags(log.Tags{
		"projection": projectionName,
	})
	return &Projector{
		nc:             nc,
		stream:         stream,
		logger:         logger,
		projectionName: projectionName,
		resumeStore:    resumeStore,
	}, nil
}

func (p *Projector) AddTopicHandler(topic string, partitions uint32, handler projection.EventHandlerFunc) {
	p.handlers = append(p.handlers, ProjectionHandler{
		topic:      topic,
		partitions: partitions,
		handler:    handler,
	})
}

// Project creates subscribes to all all events streams and process them.
// When executed the first time it will synchronously call the catchUp function
// for the projection initialisation. This function should replay all events from all event stores needed for this projection.
func (p *Projector) Project(
	ctx context.Context,
	catchUp func(context.Context, []projection.Resume) error,
) (<-chan struct{}, error) {
	if len(p.handlers) == 0 {
		return nil, faults.Errorf("no handlers defined for projector %s", p.projectionName)
	}

	var subscribers []*Subscriber
	consumerFactory := func(ctx context.Context, resume projection.ResumeKey) (projection.Consumer, error) {
		sub := NewSubscriber(p.logger, p.stream, p.resumeStore, resume)
		subscribers = append(subscribers, sub)
		return sub, nil
	}

	for _, h := range p.handlers {
		// create workers according to the topic that we want to listen
		w, err := projection.UnmanagedWorkers(ctx, p.logger, p.projectionName, h.topic, h.partitions, consumerFactory, h.handler)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		p.workers = append(p.workers, w...)
	}

	done := make(chan struct{})

	err := p.catchUp(ctx, subscribers, catchUp)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	err = p.boot(ctx, done)
	return done, faults.Wrap(err)
}

func (p *Projector) catchUp(
	ctx context.Context,
	subscribers []*Subscriber,
	catchUp func(context.Context, []projection.Resume) error,
) error {
	ok, err := p.shouldCatchup(ctx, subscribers)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	logger := p.logger.WithTags(log.Tags{
		"method": "Projector.catchUp",
	})

	logger.Info("Retrieving subscriptions last position")
	resumes, err := retrieveResumes(ctx, subscribers)
	if err != nil {
		return faults.Wrap(err)
	}

	logger.Info("Catching up projection")
	err = catchUp(ctx, resumes)
	if err != nil {
		return faults.Errorf("failed while catching up projection: %w", err)
	}

	logger.Info("Recording subscriptions positions")
	err = recordResumeTokens(ctx, subscribers, resumes)
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

func (p *Projector) shouldCatchup(ctx context.Context, subscribers []*Subscriber) (bool, error) {
	for _, sub := range subscribers {
		_, err := p.resumeStore.GetStreamResumeToken(ctx, sub.resumeKey)
		// if there is at leat one token that it is not defined it means that the last execution failed
		// and needs to be attempted again.
		if errors.Is(err, projection.ErrResumeTokenNotFound) {
			return true, nil
		}
		if err != nil {
			return false, faults.Wrap(err)
		}

	}
	return false, nil
}

func (p *Projector) boot(ctx context.Context, done chan struct{}) error {
	for _, w := range p.workers {
		w.Start(ctx)
	}

	go func() {
		<-ctx.Done()
		for _, w := range p.workers {
			w.Stop(context.Background())
		}
		close(done)
	}()

	return nil
}

func retrieveResumes(ctx context.Context, subscribers []*Subscriber) ([]projection.Resume, error) {
	var resumes []projection.Resume
	for _, sub := range subscribers {
		resume, err := sub.RetrieveLastResume(ctx)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		resumes = append(resumes, resume)
	}
	return resumes, nil
}

func recordResumeTokens(ctx context.Context, subscribers []*Subscriber, resumes []projection.Resume) error {
	for k, sub := range subscribers {
		if err := sub.RecordLastResume(ctx, resumes[k].Token); err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
