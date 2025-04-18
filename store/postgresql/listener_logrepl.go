package postgresql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	defaultSlotName = "events_pub"
	outputPlugin    = "pgoutput"
)

type FeedOption[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*Feed[K, PK])

func WithFeedEventsTable[K eventsourcing.ID, PK eventsourcing.IDPt[K]](eventsTable string) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.publicationName = fmt.Sprintf("%s_pub", eventsTable)
	}
}

func WithFeedBackoffMaxElapsedTime[K eventsourcing.ID, PK eventsourcing.IDPt[K]](duration time.Duration) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.backoffMaxElapsedTime = duration
	}
}

func WithFeedFilter[K eventsourcing.ID, PK eventsourcing.IDPt[K]](filter *store.Filter) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.filter = filter
	}
}

type Feed[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	dburl                 string
	publicationName       string
	slotIndex             int
	totalSlots            int
	backoffMaxElapsedTime time.Duration
	sinker                sink.Sinker[K]
	filter                *store.Filter
}

// NewFeed creates a new Postgresql 10+ logic replication feed.
// slotIndex is the index of this feed in a group of feeds. Its value should be between 1 and totalSlots.
// slotIndex=1 has a special maintenance behaviour of dropping any slot above totalSlots.
func NewFeed[K eventsourcing.ID, PK eventsourcing.IDPt[K]](connString string, slotIndex, totalSlots int, sinker sink.Sinker[K], options ...FeedOption[K, PK]) (Feed[K, PK], error) {
	if slotIndex < 1 || slotIndex > totalSlots {
		return Feed[K, PK]{}, faults.Errorf("slotIndex must be between 1 and %d, got %d", totalSlots, slotIndex)
	}
	f := Feed[K, PK]{
		dburl:                 connString,
		publicationName:       defaultSlotName,
		slotIndex:             slotIndex,
		totalSlots:            totalSlots,
		backoffMaxElapsedTime: 10 * time.Second,
		sinker:                sinker,
	}

	for _, o := range options {
		o(&f)
	}

	return f, nil
}

// Run listens to replication logs and pushes them to sinker
// https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
func (f *Feed[K, PK]) Run(ctx context.Context) error {
	var lastResumeToken pglogrepl.LSN // from the last position
	err := f.sinker.ResumeTokens(ctx, func(resumeToken encoding.Base64) error {
		xLogPos, err := pglogrepl.ParseLSN(resumeToken.AsString())
		if err != nil {
			return faults.Errorf("ParseLSN failed: %w", err)
		}

		// looking for the highest sequence in all partitions.
		// Sending a message to partitions is done synchronously and in order, so we should start from the last successful sent message.
		if xLogPos > lastResumeToken {
			lastResumeToken = xLogPos
		}
		return nil
	})
	if err != nil {
		return faults.Wrap(err)
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

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = f.backoffMaxElapsedTime

	return backoff.Retry(func() error {
		relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
		typeMap := pgtype.NewMap()
		inStream := false

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

					event, is, err := f.parse(xld.WALData, relationsV2, typeMap, inStream, xld.WALStart < lastResumeToken)
					if err != nil {
						return faults.Wrap(backoff.Permanent(err))
					}
					inStream = is

					clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

					if event == nil {
						continue
					}
					err = f.sinker.Sink(context.Background(), event, sink.Meta{ResumeToken: []byte(clientXLogPos.String())})
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

func (f Feed[K, PK]) parse(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream, skip bool) (*eventsourcing.Event[K], bool, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, inStream)
	if err != nil {
		return nil, false, faults.Errorf("error parsing %s: %w", string(walData), err)
	}

	switch v := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[v.RelationID] = v
	case *pglogrepl.InsertMessageV2:
		if skip {
			return nil, inStream, nil
		}
		rel, ok := relations[v.RelationID]
		if !ok {
			return nil, false, faults.Errorf("unknown relation ID %d", v.RelationID)
		}
		values := map[string]any{}
		for idx, col := range v.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': // text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return nil, false, faults.Errorf("error decoding column data: %w", err)
				}
				values[colName] = val
			}
		}

		var aggregateIDHash int32
		err = extract(values, map[string]any{
			"aggregate_id_hash": &aggregateIDHash,
		})
		if err != nil {
			return nil, false, faults.Wrap(err)
		}

		var id string
		var aggregateID string
		var aggregateVersion int32
		var aggregateKind string
		var kind string
		body := []byte{}
		var createdAt time.Time
		var migrated bool
		err = extract(values, map[string]any{
			"id":                &id,
			"aggregate_id":      &aggregateID,
			"aggregate_version": &aggregateVersion,
			"aggregate_kind":    &aggregateKind,
			"kind":              &kind,
			"body":              &body,
			"created_at":        &createdAt,
			"migrated":          &migrated,
		})
		if err != nil {
			return nil, false, faults.Wrap(err)
		}
		disc, err := extractDiscriminator(values)
		if err != nil {
			return nil, false, faults.Wrap(err)
		}

		eid, err := eventid.Parse(id)
		if err != nil {
			return nil, false, faults.Wrap(err)
		}
		aggID := PK(new(K))
		err = aggID.UnmarshalText([]byte(aggregateID))
		if err != nil {
			return nil, false, faults.Errorf("unmarshaling id '%s': %w", aggregateID, err)
		}
		e := &eventsourcing.Event[K]{
			ID:               eid,
			AggregateID:      *aggID,
			AggregateIDHash:  uint32(aggregateIDHash),
			AggregateVersion: uint32(aggregateVersion),
			AggregateKind:    eventsourcing.Kind(aggregateKind),
			Kind:             eventsourcing.Kind(kind),
			Body:             body,
			Discriminator:    disc,
			CreatedAt:        createdAt,
			Migrated:         migrated,
		}

		// check if the event is to be forwarded to the sinker
		if !f.accepts(e) {
			return nil, inStream, nil
		}

		return e, inStream, nil
	case *pglogrepl.StreamStartMessageV2:
		inStream = true
	case *pglogrepl.StreamStopMessageV2:
		inStream = false
	}
	return nil, inStream, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		v, err := dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return nil, faults.Errorf("error decoding column data: %w", err)
		}
		return v, nil
	}
	return string(data), nil
}

func extract(values map[string]any, targets map[string]any) error {
	for k, v := range targets {
		val := values[k]
		if val == nil {
			continue
		}
		err := assignTo(val, v)
		if err != nil {
			return faults.Errorf("failed to assign %s: %w", k, err)
		}
	}
	return nil
}

func assignTo(a, b any) error {
	var ok bool
	switch b := b.(type) {
	case *int32:
		*b, ok = a.(int32)
	case *int:
		*b, ok = a.(int)
	case *string:
		*b, ok = a.(string)
	case *time.Time:
		*b, ok = a.(time.Time)
	case *[]byte:
		*b, ok = a.([]byte)
	case *bool:
		*b, ok = a.(bool)
	default:
		return faults.Errorf("unsupported type %T", b)
	}

	if !ok {
		return faults.Errorf("failed to assign %T to %T", a, b)
	}

	return nil
}

func extractDiscriminator(values map[string]any) (eventsourcing.Discriminator, error) {
	meta := eventsourcing.Discriminator{}
	for k, v := range values {
		if v == nil || !strings.HasPrefix(k, store.DiscriminatorColumnPrefix) {
			continue
		}

		var s string
		err := assignTo(v, &s)
		if err != nil {
			return nil, faults.Errorf("failed to assign %s: %w", k, err)
		}
		meta[k[len(store.DiscriminatorColumnPrefix):]] = s

	}
	return meta, nil
}

// accepts check if the event is to be forwarded to the sinker
func (f *Feed[K, PK]) accepts(event *eventsourcing.Event[K]) bool {
	if f.filter == nil {
		return true
	}

	if f.filter.Splits > 1 && len(f.filter.SplitIDs) != int(f.filter.Splits) {
		p := util.CalcPartition(event.AggregateIDHash, f.filter.Splits)
		if !slices.Contains(f.filter.SplitIDs, p) {
			return false
		}
	}

	if len(f.filter.AggregateKinds) > 0 && !slices.Contains(f.filter.AggregateKinds, event.AggregateKind) {
		return false
	}

	for _, v := range f.filter.Discriminator {
		val := event.Discriminator[v.Key]
		if val == "" || !slices.Contains(v.Values, val) {
			return false
		}
	}

	return true
}

func (f *Feed[K, PK]) listReplicationSlot(ctx context.Context, conn *pgconn.PgConn) (map[string]bool, error) {
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

func (f *Feed[K, PK]) dropSlotsInExcess(ctx context.Context, conn *pgconn.PgConn, slots map[string]bool) error {
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
