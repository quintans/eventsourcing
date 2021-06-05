package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/pgtype"
	"github.com/kyleconroy/pgoutput"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

const defaultSlotName = "events_pub"

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
		p.slotName = publicationName
	}
}

func WithBackoffMaxElapsedTime(duration time.Duration) FeedLogreplOption {
	return func(p *FeedLogrepl) {
		p.backoffMaxElapsedTime = duration
	}
}

type FeedLogrepl struct {
	dburl                 string
	partitions            uint32
	partitionsLow         uint32
	partitionsHi          uint32
	slotName              string
	backoffMaxElapsedTime time.Duration
}

func NewFeed(connString string, options ...FeedLogreplOption) FeedLogrepl {
	f := FeedLogrepl{
		dburl:                 connString,
		slotName:              defaultSlotName,
		backoffMaxElapsedTime: 10 * time.Second,
	}

	for _, o := range options {
		o(&f)
	}

	return f
}

// Feed listens to replication logs and pushes them to sinker
// https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
func (f FeedLogrepl) Feed(ctx context.Context, sinker sink.Sinker) error {
	var lastResumeToken pglogrepl.LSN // from the last position
	err := store.ForEachResumeTokenInSinkPartitions(ctx, sinker, f.partitionsLow, f.partitionsHi, func(message *eventsourcing.Event) error {
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

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", f.slotName)}
	err = pglogrepl.StartReplication(ctx, conn, f.slotName, lastResumeToken, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
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
					event.ResumeToken = []byte(clientXLogPos.String())
					err = sinker.Sink(context.Background(), *event)
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
		if f.partitions > 0 {
			// check if the event is to be forwarded to the sinker
			part := common.WhichPartition(uint32(aggregateIDHash), f.partitions)
			if part < f.partitionsLow || part > f.partitionsHi {
				// we exit the loop because all rows are for the same aggregate
				return nil, nil
			}
		}

		var id string
		var aggregateID string
		var aggregateVersion int32
		var aggregateType string
		var kind string
		body := []byte{}
		var idempotencyKey string
		var metadata string
		var createdAt time.Time
		err = extract(values, map[string]interface{}{
			"id":                &id,
			"aggregate_id":      &aggregateID,
			"aggregate_version": &aggregateVersion,
			"aggregate_type":    &aggregateType,
			"kind":              &kind,
			"body":              &body,
			"idempotency_key":   &idempotencyKey,
			"metadata":          &metadata,
			"created_at":        &createdAt,
		})
		if err != nil {
			return nil, faults.Wrap(err)
		}

		eid, err := eventid.Parse(id)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		e := eventsourcing.Event{
			ID:               eid,
			AggregateID:      aggregateID,
			AggregateIDHash:  uint32(aggregateIDHash),
			AggregateVersion: uint32(aggregateVersion),
			AggregateType:    eventsourcing.AggregateType(aggregateType),
			Kind:             eventsourcing.EventKind(kind),
			Body:             body,
			IdempotencyKey:   idempotencyKey,
			CreatedAt:        createdAt,
		}

		if metadata != "" {
			e.Metadata = map[string]interface{}{}
			err = json.Unmarshal([]byte(metadata), &e.Metadata)
			if err != nil {
				return nil, faults.Errorf("failed to unmarshal metadata %s: %s", metadata, err)
			}
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
