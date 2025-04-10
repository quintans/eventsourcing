package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/util"
)

type Tx func(ctx context.Context, fn func(context.Context) error) error

const DiscriminatorColumnPrefix = "disc_"

type InTxHandlerContext[K eventsourcing.ID] struct {
	ctx   context.Context
	event *eventsourcing.Event[K]
}

func NewInTxHandlerContext[K eventsourcing.ID](ctx context.Context, event *eventsourcing.Event[K]) *InTxHandlerContext[K] {
	return &InTxHandlerContext[K]{
		ctx:   ctx,
		event: event,
	}
}

func (c *InTxHandlerContext[K]) Context() context.Context {
	return c.ctx
}

func (c *InTxHandlerContext[K]) Event() *eventsourcing.Event[K] {
	return c.event
}

type DiscriminatorHookKind string

const (
	OnPersist  = DiscriminatorHookKind("persist")
	OnRetrieve = DiscriminatorHookKind("retrieve")
)

type DiscriminatorHookContext struct {
	ctx           context.Context
	discriminator eventsourcing.Discriminator
	kind          DiscriminatorHookKind
}

func NewDiscriminatorHookContext(ctx context.Context, discriminator eventsourcing.Discriminator, kind DiscriminatorHookKind) *DiscriminatorHookContext {
	return &DiscriminatorHookContext{
		ctx:           ctx,
		discriminator: discriminator,
		kind:          kind,
	}
}

func (c *DiscriminatorHookContext) Context() context.Context {
	return c.ctx
}

func (c *DiscriminatorHookContext) Kind() DiscriminatorHookKind {
	return c.kind
}

func (c *DiscriminatorHookContext) Discriminator() eventsourcing.Discriminator {
	return c.discriminator
}

type (
	InTxHandler[K eventsourcing.ID]       func(*InTxHandlerContext[K]) error
	DiscriminatorHook[K eventsourcing.ID] func(*DiscriminatorHookContext) eventsourcing.Discriminator
)

type Filter struct {
	AggregateKinds []eventsourcing.Kind
	// Discriminator filters on top of discriminator. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Discriminator DiscriminatorFilter
	Splits        uint32
	SplitIDs      []uint32
}

type FilterOption func(*Filter)

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = filter.AggregateKinds
		f.Discriminator = filter.Discriminator
		f.Splits = filter.Splits
		f.SplitIDs = filter.SplitIDs
	}
}

func WithAggregateKinds(at ...eventsourcing.Kind) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = at
	}
}

func WithDiscriminatorFilter(key, value string) FilterOption {
	return func(f *Filter) {
		if f.Discriminator == nil {
			f.Discriminator = DiscriminatorFilter{}
		}
		f.Discriminator.Add(key, value)
	}
}

type (
	DiscriminatorFilter []*DiscriminatorKVs
	DiscriminatorKVs    struct {
		Key    string
		Values []string
	}
)

func (m *DiscriminatorFilter) Add(key string, values ...string) {
	for _, v := range *m {
		if v.Key == key {
			v.Values = append(v.Values, values...)
			return
		}
	}
	*m = append(*m, &DiscriminatorKVs{Key: key, Values: values})
}

func WithDiscriminator(filter DiscriminatorFilter) FilterOption {
	return func(f *Filter) {
		f.Discriminator = filter
	}
}

func WithSplits(partitions uint32, partitionIDs []uint32) FilterOption {
	return func(f *Filter) {
		f.Splits = partitions
		f.SplitIDs = partitionIDs
	}
}

type AggregateMetadata[K eventsourcing.ID] struct {
	Type      eventsourcing.Kind
	ID        K
	Version   uint32
	UpdatedAt time.Time
}

var ErrResumeTokenNotFound = errors.New("resume token not found")

type KVRStore interface {
	// Get retrieves the stored value for a key.
	// If the a resume key is not found it return ErrResumeTokenNotFound as an error
	Get(ctx context.Context, key string) (string, error)
}

type KVWStore interface {
	Put(ctx context.Context, key string, token string) error
}

type KVStore interface {
	KVRStore
	KVWStore
}

type Discriminator struct {
	Key   string
	Value string
}

// NilString converts nil to empty string
type NilString string

func (ns *NilString) Scan(value interface{}) error {
	if value == nil {
		*ns = ""
		return nil
	}

	switch s := value.(type) {
	case string:
		*ns = NilString(s)
	case []byte:
		*ns = NilString(s)
	}
	return nil
}

func (ns NilString) Value() (driver.Value, error) {
	if ns == "" {
		return nil, nil
	}
	return string(ns), nil
}

func DiscriminatorMerge(
	ctx context.Context,
	allowedKeys map[string]struct{},
	rootDiscriminator eventsourcing.Discriminator,
	DiscriminatorHook func(*DiscriminatorHookContext) eventsourcing.Discriminator,
	kind DiscriminatorHookKind,
) eventsourcing.Discriminator {
	disc := eventsourcing.DiscriminatorFromCtx(ctx)

	if DiscriminatorHook != nil {
		disc = DiscriminatorHook(NewDiscriminatorHookContext(ctx, disc, kind))
	}

	disc = util.MapMerge(rootDiscriminator, disc)

	return filterOut(allowedKeys, disc)
}

func filterOut(allowedKeys map[string]struct{}, m eventsourcing.Discriminator) eventsourcing.Discriminator {
	if len(allowedKeys) == 0 {
		return eventsourcing.Discriminator{}
	}

	disc := eventsourcing.Discriminator{}

	for k, v := range m {
		_, ok := allowedKeys[k]
		if ok {
			disc[k] = v
		}
	}

	return disc
}
