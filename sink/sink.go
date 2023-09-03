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
	Partitions() (uint32, []uint32)
	Accepts(hash uint32) bool
	Sink(ctx context.Context, e *eventsourcing.Event, meta Meta) error
	ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) error
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
	Encode(evt *eventsourcing.Event) ([]byte, error)
}

type Decoder interface {
	Decode([]byte) (*Message, error)
}

type Message struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Kind             eventsourcing.Kind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         *encoding.JSON
	CreatedAt        time.Time
}

type messageJSON struct {
	ID               eventid.EventID    `json:"id,omitempty"`
	AggregateID      string             `json:"aggregate_id,omitempty"`
	AggregateVersion uint32             `json:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `json:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind `json:"kind,omitempty"`
	Body             encoding.Base64    `json:"body,omitempty"`
	IdempotencyKey   string             `json:"idempotency_key,omitempty"`
	Metadata         *encoding.JSON     `json:"metadata,omitempty"`
	CreatedAt        time.Time          `json:"created_at,omitempty"`
}

type JSONCodec struct{}

func (JSONCodec) Encode(e *eventsourcing.Event) ([]byte, error) {
	msg := ToMessage(e)
	msgJ := messageJSON(*msg)
	b, err := json.Marshal(msgJ)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (JSONCodec) Decode(data []byte) (*Message, error) {
	e := &messageJSON{}
	err := json.Unmarshal(data, e)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	m := Message(*e)
	return &m, nil
}

func ToMessage(e *eventsourcing.Event) *Message {
	return &Message{
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
