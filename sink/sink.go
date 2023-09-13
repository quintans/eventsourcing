package sink

import (
	"context"
	"encoding/json"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/faults"
)

type Sinker[K eventsourcing.ID] interface {
	Sink(ctx context.Context, e *eventsourcing.Event[K], meta Meta) error
	ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) error
	Close()
}

type Codec[K eventsourcing.ID] interface {
	Encoder[K]
	Decoder[K]
}

type Meta struct {
	ResumeToken encoding.Base64
}

type Encoder[K eventsourcing.ID] interface {
	Encode(evt *eventsourcing.Event[K]) ([]byte, error)
}

type Decoder[K eventsourcing.ID] interface {
	Decode([]byte) (*Message[K], error)
}

type Message[K eventsourcing.ID] struct {
	ID               eventid.EventID
	AggregateID      K
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Kind             eventsourcing.Kind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         eventsourcing.Metadata
	CreatedAt        time.Time
}

type messageJSON struct {
	ID               eventid.EventID        `json:"id,omitempty"`
	AggregateID      string                 `json:"aggregate_id,omitempty"`
	AggregateVersion uint32                 `json:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind     `json:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind     `json:"kind,omitempty"`
	Body             encoding.Base64        `json:"body,omitempty"`
	IdempotencyKey   string                 `json:"idempotency_key,omitempty"`
	Metadata         eventsourcing.Metadata `json:"metadata,omitempty"`
	CreatedAt        time.Time              `json:"created_at,omitempty"`
}

type JSONCodec[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct{}

func (JSONCodec[K, PK]) Encode(e *eventsourcing.Event[K]) ([]byte, error) {
	msgJ := messageJSON{
		ID:               e.ID,
		AggregateID:      e.AggregateID.String(),
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
	b, err := json.Marshal(msgJ)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (JSONCodec[K, PK]) Decode(data []byte) (*Message[K], error) {
	e := &messageJSON{}
	err := json.Unmarshal(data, e)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	id := PK(new(K))
	err = id.UnmarshalText([]byte(e.AggregateID))
	if err != nil {
		return nil, faults.Errorf("unmarshaling id '%s': %w", e.AggregateID, err)
	}
	m := Message[K]{
		ID:               e.ID,
		AggregateID:      *id,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}

	return &m, nil
}

func ToMessage[K eventsourcing.ID](e *eventsourcing.Event[K]) *Message[K] {
	return &Message[K]{
		ID:               e.ID,
		AggregateID:      e.AggregateID,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
}
