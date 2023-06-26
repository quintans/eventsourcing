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

type Sinker interface {
	Sink(ctx context.Context, e *eventsourcing.Event, m Meta) (uint64, error)
	LastMessage(ctx context.Context, partition uint32) (uint64, *Message, error)
	Close()
}

type Codec interface {
	Encoder
	Decoder
}

type Meta struct {
	ResumeToken encoding.Base64
}

type Encoder interface {
	Encode(evt *eventsourcing.Event, meta Meta) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (*Message, error)
}

type Message struct {
	ID               eventid.EventID    `json:"id,omitempty"`
	ResumeToken      encoding.Base64    `json:"resume_token,omitempty"`
	AggregateID      string             `json:"aggregate_id,omitempty"`
	AggregateIDHash  uint32             `json:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32             `json:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `json:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind `json:"kind,omitempty"`
	Body             encoding.Base64    `json:"body,omitempty"`
	IdempotencyKey   string             `json:"idempotency_key,omitempty"`
	Metadata         *encoding.JSON     `json:"metadata,omitempty"`
	CreatedAt        time.Time          `json:"created_at,omitempty"`
}

type JSONCodec struct{}

func (JSONCodec) Encode(e *eventsourcing.Event, m Meta) ([]byte, error) {
	event := ToMessage(e, m)
	b, err := json.Marshal(event)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (JSONCodec) Decode(data []byte) (*Message, error) {
	e := &Message{}
	err := json.Unmarshal(data, e)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return e, nil
}

func ToMessage(e *eventsourcing.Event, meta Meta) *Message {
	return &Message{
		ID:               e.ID,
		ResumeToken:      meta.ResumeToken,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		IdempotencyKey:   e.IdempotencyKey,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
	}
}
