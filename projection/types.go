package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/dist"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

// ResumeKey is used to retrieve the last event id to replay messages directly from the event store.
type ResumeKey struct {
	topic     string
	partition uint32
	// projection identifies a projection name for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	projection string
}

func NewResumeKey(projectionName, topic string, partition uint32) (_ ResumeKey, er error) {
	defer faults.Catch(&er, "NewResumeKey(projectionName=%s, topic=%s, partition=%d)", projectionName, topic, partition)

	if projectionName == "" {
		return ResumeKey{}, faults.New("projection name cannot be empty")
	}
	if partition == 0 {
		return ResumeKey{}, faults.New("partition cannot be 0")
	}

	return ResumeKey{
		topic:      topic,
		partition:  partition,
		projection: projectionName,
	}, nil
}

func (r ResumeKey) Topic() string {
	return r.topic
}

func (r ResumeKey) Partition() uint32 {
	return r.partition
}

func (r ResumeKey) Projection() string {
	return r.projection
}

func (r ResumeKey) String() string {
	return fmt.Sprintf("%s:%s#%d", r.projection, r.topic, r.partition)
}

type ConsumerOptions[K eventsourcing.ID] struct {
	Filter  func(e *sink.Message[K]) bool
	AckWait time.Duration
}

type ConsumerOption[K eventsourcing.ID] func(*ConsumerOptions[K])

func WithFilter[K eventsourcing.ID](filter func(e *sink.Message[K]) bool) ConsumerOption[K] {
	return func(o *ConsumerOptions[K]) {
		o.Filter = filter
	}
}

func WithAckWait[K eventsourcing.ID](ackWait time.Duration) ConsumerOption[K] {
	return func(o *ConsumerOptions[K]) {
		o.AckWait = ackWait
	}
}

type Consumer[K eventsourcing.ID] interface {
	Topic() string
	// returns the subscriber Positions. The first Position should be 1
	StartConsumer(ctx context.Context, startTime *time.Time, projectionName string, handle ConsumerHandler[K], options ...ConsumerOption[K]) error
}

type ConsumerHandler[K eventsourcing.ID] func(ctx context.Context, e *sink.Message[K], partition uint32, seq uint64) error

type ConsumerTopic struct {
	Topic      string
	Partitions []uint32
}

type Event[K eventsourcing.ID] struct {
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

func FromEvent[K eventsourcing.ID](e *eventsourcing.Event[K]) *Event[K] {
	return &Event[K]{
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

func FromMessage[K eventsourcing.ID](m *sink.Message[K]) *Event[K] {
	return &Event[K]{
		ID:               m.ID,
		AggregateID:      m.AggregateID,
		AggregateVersion: m.AggregateVersion,
		AggregateKind:    m.AggregateKind,
		Kind:             m.Kind,
		Body:             m.Body,
		IdempotencyKey:   m.IdempotencyKey,
		Metadata:         m.Metadata,
		CreatedAt:        m.CreatedAt,
	}
}

type TokenKind string

const (
	CatchUpToken  TokenKind = "catchup"
	ConsumerToken TokenKind = "consumer"
)

type tokenDB struct {
	Kind     TokenKind       `json:"kind"`
	EventID  eventid.EventID `json:"eventID,omitempty"`
	Sequence uint64          `json:"sequence,omitempty"`
}

type Token struct {
	kind     TokenKind
	eventID  eventid.EventID
	sequence uint64
}

func NewConsumerToken(sequence uint64, eventID eventid.EventID) Token {
	return Token{
		kind:     ConsumerToken,
		sequence: sequence,
		eventID:  eventID,
	}
}

func NewCatchupToken(eventID eventid.EventID) Token {
	return Token{
		kind:    CatchUpToken,
		eventID: eventID,
	}
}

func ParseToken(s string) (_ Token, e error) {
	defer faults.Catch(&e, "projection.ParseToken(token=%s)", s)

	if s == "" {
		return Token{}, nil
	}

	t := tokenDB{}
	err := json.Unmarshal([]byte(s), &t)
	if err != nil {
		return Token{}, faults.Errorf("parsing token data '%s': %w", s, err)
	}

	if !slices.Contains([]TokenKind{CatchUpToken, ConsumerToken}, t.Kind) {
		return Token{}, faults.Errorf("invalid kind when parsing token: %s", s)
	}

	return Token{
		kind:     t.Kind,
		eventID:  t.EventID,
		sequence: t.Sequence,
	}, nil
}

func (t Token) String() string {
	// don't want to return an error, so I use a poor man marshalling
	return fmt.Sprintf(`{"kind": "%s", "eventID": "%s", "sequence": %d}`, t.kind, t.eventID, t.sequence)
}

func (t Token) Kind() TokenKind {
	return t.kind
}

func (t Token) ConsumerSequence() uint64 {
	return t.sequence
}

func (t Token) CatchupEventID() eventid.EventID {
	return t.eventID
}

func (t Token) IsEmpty() bool {
	return t.sequence == 0 && t.eventID.IsZero()
}

func (t Token) IsZero() bool {
	return t == Token{}
}

type Projection[K eventsourcing.ID] interface {
	Name() string
	CatchUpOptions() CatchUpOptions
	Handle(ctx context.Context, e Message[K]) error
}

type Message[K eventsourcing.ID] struct {
	Meta    Meta
	Message *sink.Message[K]
}

type MessageKind string

var (
	MessageKindCatchup MessageKind = "catchup"
	MessageKindSwitch  MessageKind = "switch"
	MessageKindLive    MessageKind = "live"
)

type Meta struct {
	Name      string
	Kind      MessageKind
	Partition uint32
	Sequence  uint64
}

type (
	LockerFactory     func(lockName string) dist.Locker
	WaitLockerFactory func(lockName string) dist.WaitLocker
	CatchUpCallback   func(context.Context, eventid.EventID) (eventid.EventID, error)
)

type CatchUpOptions struct {
	StartOffset   time.Duration
	CatchUpWindow time.Duration

	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata store.MetadataFilter
}
