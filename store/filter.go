package store

type Filter struct {
	AggregateTypes []string
	// Labels filters on top of labels. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Labels []Label
}

func NewLabel(key, value string) Label {
	return Label{
		Key:   key,
		Value: value,
	}
}

type Label struct {
	Key   string
	Value string
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
			f.Labels = []Label{}
		}
		f.Labels = append(f.Labels, NewLabel(key, value))
	}
}

type Labels map[string]string

func WithLabels(labels Labels) FilterOption {
	return func(f *Filter) {
		f.Labels = make([]Label, len(labels))
		idx := 0
		for k, v := range labels {
			f.Labels[idx] = NewLabel(k, v)
			idx++
		}
	}
}
