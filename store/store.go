package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/quintans/eventsourcing"
)

type Tx func(ctx context.Context, fn func(context.Context) error) error

const MetaColumnPrefix = "meta_"

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

type MetadataHookKind string

const (
	OnPersist  = MetadataHookKind("persist")
	OnRetrieve = MetadataHookKind("retrieve")
)

type MetadataHookContext struct {
	ctx  context.Context
	kind MetadataHookKind
}

func NewMetadataHookContext(ctx context.Context, kind MetadataHookKind) *MetadataHookContext {
	return &MetadataHookContext{
		ctx:  ctx,
		kind: kind,
	}
}

func (c *MetadataHookContext) Context() context.Context {
	return c.ctx
}

func (c *MetadataHookContext) Kind() MetadataHookKind {
	return c.kind
}

type (
	InTxHandler[K eventsourcing.ID]  func(*InTxHandlerContext[K]) error
	MetadataHook[K eventsourcing.ID] func(*MetadataHookContext) eventsourcing.Metadata
)

type Filter struct {
	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata MetadataFilter
	Splits   uint32
	SplitIDs []uint32
}

type FilterOption func(*Filter)

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = filter.AggregateKinds
		f.Metadata = filter.Metadata
		f.Splits = filter.Splits
		f.SplitIDs = filter.SplitIDs
	}
}

func WithAggregateKinds(at ...eventsourcing.Kind) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = at
	}
}

func WithMetadataFilter(key, value string) FilterOption {
	return func(f *Filter) {
		if f.Metadata == nil {
			f.Metadata = MetadataFilter{}
		}
		f.Metadata.Add(key, value)
	}
}

type (
	MetadataFilter []*MetadataKVs
	MetadataKVs    struct {
		Key    string
		Values []string
	}
)

func (m *MetadataFilter) Add(key string, values ...string) {
	for _, v := range *m {
		if v.Key == key {
			v.Values = append(v.Values, values...)
			return
		}
	}
	*m = append(*m, &MetadataKVs{Key: key, Values: values})
}

func WithMetadata(metadata MetadataFilter) FilterOption {
	return func(f *Filter) {
		f.Metadata = metadata
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

type Metadata struct {
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
