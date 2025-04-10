package sink

import (
	"context"
	"encoding/json"
	"fmt"
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
	Discriminator    eventsourcing.Discriminator
	CreatedAt        time.Time
}

func (m *Message[K]) String() string {
	return fmt.Sprintf(`{ID: %s, AggregateID: %s, AggregateVersion: %d, AggregateKind: %s, Kind: %s, Body: %s, Discriminator: %s, CreatedAt: %s}`,
		m.ID,
		m.AggregateID,
		m.AggregateVersion,
		m.AggregateKind,
		m.Kind,
		m.Body,
		m.Discriminator,
		m.CreatedAt,
	)
}

type messageJSON struct {
	ID               eventid.EventID             `json:"id,omitempty"`
	AggregateID      string                      `json:"aggregate_id,omitempty"`
	AggregateVersion uint32                      `json:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind          `json:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind          `json:"kind,omitempty"`
	Body             encoding.Base64             `json:"body,omitempty"`
	Discriminator    eventsourcing.Discriminator `json:"discriminator,omitempty"`
	CreatedAt        time.Time                   `json:"created_at,omitempty"`
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
		Discriminator:    e.Discriminator,
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
		Discriminator:    e.Discriminator,
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
		Discriminator:    e.Discriminator,
		CreatedAt:        e.CreatedAt,
	}
}
