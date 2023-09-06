package store

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/eventsourcing"
)

type Filter struct {
	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata Metadata
	Splits   uint32
	Split    uint32
}

type FilterOption func(*Filter)

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = filter.AggregateKinds
		f.Metadata = filter.Metadata
		f.Splits = filter.Splits
		f.Split = filter.Split
	}
}

func WithAggregateKinds(at ...eventsourcing.Kind) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = at
	}
}

func WithMetadataKV(key, value string) FilterOption {
	return func(f *Filter) {
		if f.Metadata == nil {
			f.Metadata = Metadata{}
		}
		values := f.Metadata[key]
		if values == nil {
			values = []string{value}
		} else {
			values = append(values, value)
		}
		f.Metadata[key] = values
	}
}

type Metadata map[string][]string

func WithMetadata(metadata Metadata) FilterOption {
	return func(f *Filter) {
		f.Metadata = metadata
	}
}

func WithPartitions(partitions, partition uint32) FilterOption {
	return func(f *Filter) {
		f.Splits = partitions
		f.Split = partition
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
