package eventstore

import (
	"context"
	"errors"

	"github.com/quintans/eventstore/common"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type Options struct {
	IdempotencyKey string
	// Labels tags the event. eg: {"geo": "EU"}
	Labels map[string]string
}

type EventStore interface {
	GetByID(ctx context.Context, aggregateID string, aggregate common.Aggregater) error
	Save(ctx context.Context, aggregate common.Aggregater, options Options) error
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest) error
}
