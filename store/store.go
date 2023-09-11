package store

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/eventsourcing"
)

const MetaColumnPrefix = "meta_"

type Filter struct {
	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata MetadataFilter
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

type Metadata struct {
	Key   string
	Value string
}
