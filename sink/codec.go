package sink

import (
	"encoding/json"
	"time"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventstore"
	"github.com/quintans/faults"
)

type Codec interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(eventstore.Event) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (eventstore.Event, error)
}

type Event struct {
	ID               string                 `json:"id,omitempty"`
	ResumeToken      encoding.Base64        `json:"resume_token,omitempty"`
	AggregateID      string                 `json:"aggregate_id,omitempty"`
	AggregateIDHash  uint32                 `json:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32                 `json:"aggregate_version,omitempty"`
	AggregateType    string                 `json:"aggregate_type,omitempty"`
	Kind             string                 `json:"kind,omitempty"`
	Body             encoding.Base64        `json:"body,omitempty"`
	IdempotencyKey   string                 `json:"idempotency_key,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt        time.Time              `json:"created_at,omitempty"`
}

type JsonCodec struct{}

func (JsonCodec) Encode(e eventstore.Event) ([]byte, error) {
	event := Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateType:    e.AggregateType,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
	b, err := json.Marshal(event)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (JsonCodec) Decode(data []byte) (eventstore.Event, error) {
	e := Event{}
	err := json.Unmarshal(data, &e)
	if err != nil {
		return eventstore.Event{}, faults.Wrap(err)
	}

	event := eventstore.Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateType:    e.AggregateType,
		Kind:             e.Kind,
		Body:             []byte(e.Body),
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
	return event, nil
}
