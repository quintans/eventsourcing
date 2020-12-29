package sink

import (
	"encoding/json"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/encoding"
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
	AggregateVersion uint32                 `json:"aggregate_version,omitempty"`
	AggregateType    string                 `json:"aggregate_type,omitempty"`
	Kind             string                 `json:"kind,omitempty"`
	Body             encoding.Base64        `json:"body,omitempty"`
	IdempotencyKey   string                 `json:"idempotency_key,omitempty"`
	Labels           map[string]interface{} `json:"labels,omitempty"`
	CreatedAt        time.Time              `json:"created_at,omitempty"`
}

type JsonCodec struct{}

func (_ JsonCodec) Encode(e eventstore.Event) ([]byte, error) {
	event := Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateVersion: e.AggregateVersion,
		AggregateType:    e.AggregateType,
		Kind:             e.Kind,
		Body:             []byte(e.Body),
		IdempotencyKey:   e.IdempotencyKey,
		Labels:           e.Labels,
		CreatedAt:        e.CreatedAt,
	}
	b, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (_ JsonCodec) Decode(data []byte) (eventstore.Event, error) {
	e := Event{}
	err := json.Unmarshal(data, &e)
	if err != nil {
		return eventstore.Event{}, err
	}

	event := eventstore.Event{
		ID:               e.ID,
		ResumeToken:      e.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateVersion: e.AggregateVersion,
		AggregateType:    e.AggregateType,
		Kind:             e.Kind,
		Body:             []byte(e.Body),
		IdempotencyKey:   e.IdempotencyKey,
		Labels:           e.Labels,
		CreatedAt:        e.CreatedAt,
	}
	return event, nil
}
