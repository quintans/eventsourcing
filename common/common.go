package common

import (
	"strings"
	"time"
)

type Filter struct {
	AggregateTypes []string
	// Labels filters on top of labels. Every key of the map is ANDed with every OR of the values
	// eg: {"geo": ["EU", "USA"], "membership": "prime"} equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Labels map[string][]string
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

type Aggregater interface {
	GetID() string
	GetVersion() int
	SetVersion(int)
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
}

type Event struct {
	ID               string
	AggregateID      string
	AggregateVersion int
	AggregateType    string
	Kind             string
	Body             Json
	IdempotencyKey   string
	Labels           Json
	CreatedAt        time.Time
}

type PgEvent struct {
	ID               string    `db:"id"`
	AggregateID      string    `db:"aggregate_id"`
	AggregateVersion int       `db:"aggregate_version"`
	AggregateType    string    `db:"aggregate_type"`
	Kind             string    `db:"kind"`
	Body             Json      `db:"body"`
	IdempotencyKey   string    `db:"idempotency_key"`
	Labels           Json      `db:"labels"`
	CreatedAt        time.Time `db:"created_at"`
}

func JoinAndEscape(s []string) string {
	fields := strings.Join(s, ", ")
	return Escape(fields)
}

func Escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
