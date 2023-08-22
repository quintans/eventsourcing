package postgresql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/kyleconroy/pgoutput"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	defaultSlotName    = "events_pub"
	outputPlugin       = "pgoutput"
	defaultEventsTable = "events"
)

type FeedLogreplOption func(*FeedLogrepl)

func WithLogRepPartitions(partitions, partitionsLow, partitionsHi uint32) FeedLogreplOption {
	return func(p *FeedLogrepl) {
		if partitions <= 1 {
			return
		}
		p.partitions = partitions
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithPublication(publicationName string) FeedLogreplOption {
	return func(p *FeedLogrepl) {
		p.publicationName = publicationName
	}
}

func WithBackoffMaxElapsedTime(duration time.Duration) FeedLogreplOption {
	return func(p *FeedLogrepl) {
		p.backoffMaxElapsedTime = duration
	}
}

func WithEventsTable(col string) FeedLogreplOption {
	return func(p *FeedLogrepl) {
		p.eventsTable = col
	}
}

type FeedLogrepl struct {
	dburl                 string
	partitions            uint32
	partitionsLow         uint32
	partitionsHi          uint32
	publicationName       string
	slotIndex             int
	totalSlots            int
	backoffMaxElapsedTime time.Duration
	sinker                sink.Sinker
	eventsTable           string
	setSeqRepo            SetSeqRepository
}

type SetSeqRepository interface {
	SetSinkData(ctx context.Context, eID eventid.EventID, data sink.Data) error
}

// NewFeed creates a new Postgresql 10+ logic replication feed.
// slotIndex is the index of this feed in a group of feeds. Its value should be between 1 and totalSlots.
// slotIndex=1 has a special maintenance behaviour of dropping any slot above totalSlots.
func NewFeed(connString string, slotIndex, totalSlots int, sinker sink.Sinker, setSeqRepo SetSeqRepository, options ...FeedLogreplOption) (FeedLogrepl, error) {
	if slotIndex < 1 || slotIndex > totalSlots {
		return FeedLogrepl{}, faults.Errorf("slotIndex must be between 1 and %d, got %d", totalSlots, slotIndex)
	}
	f := FeedLogrepl{
		dburl:                 connString,
		publicationName:       defaultSlotName,
		slotIndex:             slotIndex,
		totalSlots:            totalSlots,
		backoffMaxElapsedTime: 10 * time.Second,
		sinker:                sinker,
		eventsTable:           defaultEventsTable,
		setSeqRepo:            setSeqRepo,
	}

	for _, o := range options {
		o(&f)
	}

	return f, nil
}

// Run listens to replication logs and pushes them to sinker
// https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
func (f *FeedLogrepl) Run(ctx context.Context) error {
	var lastResumeToken pglogrepl.LSN // from the last position
	err := store.ForEachSequenceInSinkPartitions(ctx, f.sinker, f.partitionsLow, f.partitionsHi, func(_ uint64, message *sink.Message) error {
		xLogPos, err := pglogrepl.ParseLSN(string(message.ResumeToken))
		if err != nil {
			return faults.Errorf("ParseLSN failed: %w", err)
		}
		if xLogPos > lastResumeToken {
			lastResumeToken = xLogPos
		}
		return nil
	})
	if err != nil {
		return err
	}

	conn, err := pgconn.Connect(ctx, f.dburl)
	if err != nil {
		return faults.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(context.Background())

	listSlots, err := f.listReplicationSlot(ctx, conn)
	if err != nil {
		return faults.Errorf("listReplicationSlot failed: %w", err)
	}
	if err = f.dropSlotsInExcess(ctx, conn, listSlots); err != nil {
		return faults.Errorf("dropSlotsInExcess failed: %w", err)
	}

	slotName := f.publicationName + "_" + strconv.Itoa(f.slotIndex)
	if !listSlots[slotName] {
		_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			return faults.Errorf("CreateReplicationSlot failed: %w", err)
		}
	}

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", f.publicationName)}
	if err = pglogrepl.StartReplication(ctx, conn, slotName, lastResumeToken, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {
		return faults.Errorf("StartReplication failed: %w", err)
	}

	clientXLogPos := lastResumeToken
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	set := pgoutput.NewRelationSet()

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = f.backoffMaxElapsedTime

	return backoff.Retry(func() error {
		for {
			if time.Now().After(nextStandbyMessageDeadline) {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
				if err != nil {
					return faults.Errorf("SendStandbyStatusUpdate failed: %w", err)
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			ctx2, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
			msg, err := conn.ReceiveMessage(ctx2)
			cancel()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return backoff.Permanent(err)
				}
				if pgconn.Timeout(err) {
					continue
				}
				return faults.Errorf("ReceiveMessage failed: %w", err)
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						return faults.Errorf("ParsePrimaryKeepaliveMessage failed: %w", backoff.Permanent(err))
					}

					if pkm.ReplyRequested {
						nextStandbyMessageDeadline = time.Time{}
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						return faults.Errorf("ParseXLogData failed: %w", backoff.Permanent(err))
					}

					event, err := f.parse(set, xld.WALData, xld.WALStart < lastResumeToken)
					if err != nil {
						return faults.Wrap(backoff.Permanent(err))
					}

					clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

					if event == nil {
						continue
					}
					data, err := f.sinker.Sink(context.Background(), event, sink.Meta{ResumeToken: []byte(clientXLogPos.String())})
					if err != nil {
						return faults.Wrap(backoff.Permanent(err))
					}
					err = f.setSeqRepo.SetSinkData(ctx, event.ID, data)
					if err != nil {
						return faults.Wrap(backoff.Permanent(err))
					}
				}
			default:
				log.Printf("Received unexpected message: %#v\n", msg)
			}

			b.Reset()
		}
	}, b)
}

func (f FeedLogrepl) parse(set *pgoutput.RelationSet, WALData []byte, skip bool) (*eventsourcing.Event, error) {
	m, err := pgoutput.Parse(WALData)
	if err != nil {
		return nil, faults.Errorf("error parsing %s: %w", string(WALData), err)
	}

	switch v := m.(type) {
	case pgoutput.Relation:
		set.Add(v)
	case pgoutput.Insert:
		if skip {
			return nil, nil
		}
		values, err := set.Values(v.RelationID, v.Row)
		if err != nil {
			return nil, faults.Errorf("failed to get relation set values: %w", err)
		}

		var aggregateIDHash int32
		err = extract(values, map[string]interface{}{
			"aggregate_id_hash": &aggregateIDHash,
		})
		if err != nil {
			return nil, faults.Wrap(err)
		}

		// check if the event is to be forwarded to the sinker
		part, err := util.WhichPartition(uint32(aggregateIDHash), f.partitions)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		if part < f.partitionsLow || part > f.partitionsHi {
			// we exit the loop because all rows are for the same aggregate
			return nil, nil
		}

		var id string
		var aggregateID string
		var aggregateVersion int32
		var aggregateKind string
		var kind string
		body := []byte{}
		var idempotencyKey string
		var metadata string
		var createdAt time.Time
		var migrated bool
		err = extract(values, map[string]interface{}{
			"id":                &id,
			"aggregate_id":      &aggregateID,
			"aggregate_version": &aggregateVersion,
			"aggregate_kind":    &aggregateKind,
			"kind":              &kind,
			"body":              &body,
			"idempotency_key":   &idempotencyKey,
			"metadata":          &metadata,
			"created_at":        &createdAt,
			"migrated":          &migrated,
		})
		if err != nil {
			return nil, faults.Wrap(err)
		}

		eid, err := eventid.Parse(id)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		// Partition and Sequence don't need to be assigned because at this moment they have a zero value.
		// They will be populate with the values returned by the sink.
		e := eventsourcing.Event{
			ID:               eid,
			AggregateID:      aggregateID,
			AggregateIDHash:  uint32(aggregateIDHash),
			AggregateVersion: uint32(aggregateVersion),
			AggregateKind:    eventsourcing.Kind(aggregateKind),
			Kind:             eventsourcing.Kind(kind),
			Body:             body,
			Metadata:         encoding.JSONOfString(metadata),
			IdempotencyKey:   idempotencyKey,
			CreatedAt:        createdAt,
			Migrated:         migrated,
		}

		return &e, nil
	}
	return nil, nil
}

func extract(values map[string]pgtype.Value, targets map[string]interface{}) error {
	for k, v := range targets {
		val := values[k]
		if val.Get() == nil {
			continue
		}
		err := val.AssignTo(v)
		if err != nil {
			return faults.Errorf("failed to assign %s: %w", k, err)
		}
	}
	return nil
}

func (f *FeedLogrepl) listReplicationSlot(ctx context.Context, conn *pgconn.PgConn) (map[string]bool, error) {
	sql := fmt.Sprintf("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '%s%%'", f.publicationName)
	mrr := conn.Exec(ctx, sql)
	results, err := mrr.ReadAll()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	if len(results) != 1 {
		return nil, faults.Errorf("expected 1 result set for '%s', got %d", f.publicationName, len(results))
	}

	slots := map[string]bool{}
	result := results[0]
	for _, cols := range result.Rows {
		if len(cols) != 1 {
			return nil, faults.Errorf("expected 1 result column for '%s', got %d: %s", f.publicationName, len(cols), string(cols[0]))
		}

		slots[string(cols[0])] = true
	}

	return slots, nil
}

func (f *FeedLogrepl) dropSlotsInExcess(ctx context.Context, conn *pgconn.PgConn, slots map[string]bool) error {
	// we only do the clean up when the listener has index 0
	if f.slotIndex != 1 {
		return nil
	}

	idx := len(f.publicationName) + 1 // +1 to account for the _
	for k := range slots {
		i, err := strconv.Atoi(k[idx:])
		if err != nil {
			return err
		}
		if i > f.totalSlots {
			pglogrepl.DropReplicationSlot(ctx, conn, k, pglogrepl.DropReplicationSlotOptions{})
		}
	}

	return nil
}
