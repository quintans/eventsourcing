package store

import "github.com/quintans/eventstore"

type Filter struct {
	AggregateTypes []string
	// Labels filters on top of labels. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Labels       Labels
	Partitions   uint32
	PartitionLow uint32
	PartitionHi  uint32
}

type FilterOption func(*Filter)

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f = &filter
	}
}

func WithAggregateTypes(at ...string) FilterOption {
	return func(f *Filter) {
		f.AggregateTypes = at
	}
}

func WithLabel(key, value string) FilterOption {
	return func(f *Filter) {
		if f.Labels == nil {
			f.Labels = Labels{}
		}
		values := f.Labels[key]
		if values == nil {
			values = []string{value}
		} else {
			values = append(values, value)
		}
		f.Labels[key] = values
	}
}

type Labels map[string][]string

func WithLabels(labels Labels) FilterOption {
	return func(f *Filter) {
		f.Labels = labels
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

type Projector interface {
	Project(eventstore.Event)
}
