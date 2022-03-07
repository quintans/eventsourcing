package sink

import (
	"encoding/json"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
)

type Codec interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(eventsourcing.Event) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (eventsourcing.Event, error)
}

type Event struct {
	ID               eventid.EventID    `json:"id,omitempty"`
	ResumeToken      encoding.Base64    `json:"resume_token,omitempty"`
	AggregateID      string             `json:"aggregate_id,omitempty"`
	AggregateIDHash  uint32             `json:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32             `json:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `json:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind `json:"kind,omitempty"`
	Body             encoding.Base64    `json:"body,omitempty"`
	IdempotencyKey   string             `json:"idempotency_key,omitempty"`
	Metadata         *encoding.Json     `json:"metadata,omitempty"`
	CreatedAt        time.Time          `json:"created_at,omitempty"`
	Migrated         bool               `json:"migrated,omitempty"`
}

type JsonCodec struct{}

func (JsonCodec) Encode(e eventsourcing.Event) ([]byte, error) {
	event := Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
	}
	b, err := json.Marshal(event)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (JsonCodec) Decode(data []byte) (eventsourcing.Event, error) {
	e := Event{}
	err := json.Unmarshal(data, &e)
	if err != nil {
		return eventsourcing.Event{}, faults.Wrap(err)
	}
	event := eventsourcing.Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             []byte(e.Body),
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
	}
	return event, nil
}
