package store

import (
	"time"

	"github.com/quintans/eventsourcing"
)

type Filter struct {
	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata     Metadata
	Partitions   uint32
	PartitionLow uint32
	PartitionHi  uint32
}

type FilterOption func(*Filter)

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f.AggregateKinds = filter.AggregateKinds
		f.Metadata = filter.Metadata
		f.Partitions = filter.Partitions
		f.PartitionLow = filter.PartitionLow
		f.PartitionHi = filter.PartitionHi
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

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FilterOption {
	return func(f *Filter) {
		if partitions <= 1 {
			return
		}
		f.Partitions = partitions
		f.PartitionLow = partitionsLow
		f.PartitionHi = partitionsHi
	}
}

type AggregateMetadata struct {
	Type      eventsourcing.Kind
	ID        string
	Version   uint32
	UpdatedAt time.Time
}
