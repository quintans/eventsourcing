package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
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

type ConsumerOptions struct {
	Filter  func(e *sink.Message) bool
	AckWait time.Duration
}

type ConsumerOption func(*ConsumerOptions)

func WithFilter(filter func(e *sink.Message) bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Filter = filter
	}
}

func WithAckWait(ackWait time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.AckWait = ackWait
	}
}

type Consumer interface {
	TopicPartitions() (string, []uint32)
	// returns the subscriber Positions. The first Position should be 1
	Positions(ctx context.Context) (map[uint32]SubscriberPosition, error)
	ResetConsumer(ctx context.Context, projName string) error
	StartConsumer(ctx context.Context, projectionName string, handle ConsumerHandler, options ...ConsumerOption) error
}

type ConsumerHandler func(ctx context.Context, e *sink.Message, partition uint32, seq uint64) error

type ConsumerTopic struct {
	Topic      string
	Partitions []uint32
}

type SubscriberPosition struct {
	EventID  eventid.EventID
	Position uint64
}

type Event struct {
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

func FromEvent(e *eventsourcing.Event) *Event {
	return &Event{
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

func FromMessage(m *sink.Message) *Event {
	return &Event{
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

func NewConsumerToken(sequence uint64) Token {
	return Token{
		kind:     ConsumerToken,
		sequence: sequence,
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

	if !util.In(t.Kind, CatchUpToken, ConsumerToken) {
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

type Projection interface {
	Name() string
	CatchUpOptions() CatchUpOptions
	Handle(ctx context.Context, e *sink.Message) error
}

type (
	LockerFactory     func(lockName string) lock.Locker
	WaitLockerFactory func(lockName string) lock.WaitLocker
	CatchUpCallback   func(context.Context, eventid.EventID) (eventid.EventID, error)
)

type CatchUpOptions struct {
	StartOffset   time.Duration
	CatchUpWindow time.Duration

	AggregateKinds []eventsourcing.Kind
	// Metadata filters on top of metadata. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Metadata store.Metadata
}
