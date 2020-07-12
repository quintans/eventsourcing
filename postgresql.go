package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/quintans/eventstore/common"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

const (
	uniqueViolation = "23505"
)

var _ EventStore = (*ESPostgreSQL)(nil)

// NewESPostgreSQL creates a new instance of ESPostgreSQL
func NewESPostgreSQL(repo EsRepository, snapshotThreshold int) *ESPostgreSQL {
	return &ESPostgreSQL{
		repo:              repo,
		snapshotThreshold: snapshotThreshold,
	}
}

// ESPostgreSQL is the implementation of an Event Store in PostgreSQL
type ESPostgreSQL struct {
	repo              EsRepository
	snapshotThreshold int
}

func (es *ESPostgreSQL) GetByID(ctx context.Context, aggregateID string, aggregate common.Aggregater) error {
	snap, err := es.repo.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return err
	}

	var events []common.PgEvent
	if snap != nil {
		err = json.Unmarshal(snap.Body, aggregate)
		if err != nil {
			return err
		}
		events, err = es.repo.GetEvents(ctx, aggregateID, snap.AggregateVersion)
	} else {
		events, err = es.repo.GetEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return err
	}

	for _, v := range events {
		aggregate.ApplyChangeFromHistory(common.Event{
			AggregateID:      v.AggregateID,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			CreatedAt:        v.CreatedAt,
		})
	}

	return nil
}

func (es *ESPostgreSQL) Save(ctx context.Context, aggregate common.Aggregater, options Options) (err error) {
	events := aggregate.GetEvents()
	if len(events) == 0 {
		return nil
	}

	tName := nameFor(aggregate)
	version := aggregate.GetVersion()
	oldVersion := version

	var eventID string
	defer func() {
		if err != nil {
			aggregate.SetVersion(oldVersion)
			return
		}
		if version > es.snapshotThreshold-1 {
			mod := oldVersion % es.snapshotThreshold
			delta := version - oldVersion + mod
			if delta >= es.snapshotThreshold {
				var snap *PgSnapshot
				snap, err = buildSnapshot(aggregate, eventID)
				if err != nil {
					return
				}
				go es.repo.SaveSnapshot(ctx, snap)
			}
		}
	}()

	labels, err := json.Marshal(options.Labels)
	if err != nil {
		return err
	}

	size := len(events)
	if size == 0 {
		return nil
	}
	ers := make([]EventRecord, size)
	for i := 0; i < size; i++ {
		e := events[i]
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		ers[i] = EventRecord{
			AggregateID: aggregate.GetID(),
			Version:     aggregate.GetVersion(),
			Name:        nameFor(e),
			Body:        body,
		}
	}
	eventID, err = es.repo.SaveEvent(ctx, tName, ers, options.IdempotencyKey, labels)
	if err != nil {
		return err
	}

	aggregate.ClearEvents()
	return nil
}

func buildSnapshot(agg common.Aggregater, eventID string) (*PgSnapshot, error) {
	body, err := json.Marshal(agg)
	if err != nil {
		log.WithField("aggregate", agg).
			WithError(err).
			Error("Failed to serialize aggregate")
		return nil, err
	}

	return &PgSnapshot{
		ID:               eventID,
		AggregateID:      agg.GetID(),
		AggregateVersion: agg.GetVersion(),
		Body:             body,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

func nameFor(x interface{}) string {
	t := reflect.TypeOf(x)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func (es *ESPostgreSQL) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	return es.repo.HasIdempotencyKey(ctx, aggregateID, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID     string
	AggregateFields []string
	Events          []EventKind
}

type EventKind struct {
	Kind   string
	Fields []string
}

func (es *ESPostgreSQL) Forget(ctx context.Context, request ForgetRequest) error {
	return es.repo.Forget(ctx, request)
}

type EsRepository interface {
	SaveEvent(ctx context.Context, tName string, eRec []EventRecord, idempotencyKey string, labels []byte) (string, error)
	GetSnapshot(ctx context.Context, aggregateID string) (*PgSnapshot, error)
	SaveSnapshot(ctx context.Context, snap interface{})
	GetEvents(ctx context.Context, aggregateID string, snapVersion int) ([]common.PgEvent, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest) error
}

type PgEsRepository struct {
	db *sqlx.DB
}

// NewESPostgreSQL creates a new instance of ESPostgreSQL
func NewPgEsRepository(dburl string) (*PgEsRepository, error) {
	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PgEsRepository{
		db: db,
	}, nil
}

type EventRecord struct {
	AggregateID string
	Version     int
	Name        string
	Body        []byte
}

func (r *PgEsRepository) SaveEvent(ctx context.Context, tName string, eRec []EventRecord, idempotencyKey string, labels []byte) (string, error) {
	var eID string
	er := r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		var eventID string
		for _, e := range eRec {
			eventID = xid.New().String()
			now := time.Now().UTC()

			_, err := tx.ExecContext(c,
				`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body, idempotency_key, labels, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				eventID, e.AggregateID, e.Version, tName, e.Name, e.Body, idempotencyKey, labels, now)
			if err != nil {
				if pgerr, ok := err.(*pq.Error); ok {
					if pgerr.Code == uniqueViolation {
						return ErrConcurrentModification
					}
				}
				return fmt.Errorf("Unable to insert event: %w", err)
			}
		}
		eID = eventID
		return nil
	})
	return eID, er
}

func (r *PgEsRepository) GetSnapshot(ctx context.Context, aggregateID string) (*PgSnapshot, error) {
	snap := &PgSnapshot{}
	if err := r.db.GetContext(ctx, snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return snap, nil
}

func (r *PgEsRepository) SaveSnapshot(ctx context.Context, snap interface{}) {
	_, err := r.db.NamedExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, body, created_at)
	     VALUES (:id, :aggregate_id, :aggregate_version, :body, :created_at)`, snap)

	if err != nil {
		log.WithField("snapshot", snap).
			WithError(err).
			Error("Failed to save snapshot")
	}
}

func (r *PgEsRepository) GetEvents(ctx context.Context, aggregateID string, snapVersion int) ([]common.PgEvent, error) {
	query := "SELECT * FROM events e WHERE e.aggregate_id = $1"
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query += " AND e.aggregate_version > $2"
		args = append(args, snapVersion)
	}
	events := []common.PgEvent{}
	if err := r.db.SelectContext(ctx, &events, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Aggregate '%s' was not found: %w", aggregateID, err)
		}
		return nil, fmt.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}
	return events, nil
}

func (r *PgEsRepository) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r *PgEsRepository) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	var exists int
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=$1 AND aggregate_type=$2) AS "EXISTS"`, idempotencyKey, aggregateID)
	if err != nil {
		return false, fmt.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists != 0, nil
}

func (r *PgEsRepository) Forget(ctx context.Context, request ForgetRequest) error {
	for _, evt := range request.Events {
		sql := common.JoinAndEscape(evt.Fields)
		sql = fmt.Sprintf("UPDATE events SET body =  body - '{%s}'::text[] WHERE aggregate_id = $1 AND kind = $2", sql)
		_, err := r.db.ExecContext(ctx, sql, request.AggregateID, evt.Kind)
		if err != nil {
			return fmt.Errorf("Unable to forget events: %w", err)
		}
	}

	sql := common.JoinAndEscape(request.AggregateFields)
	sql = fmt.Sprintf("UPDATE snapshots SET body =  body - '{%s}'::text[] WHERE aggregate_id = $1", sql)
	_, err := r.db.ExecContext(ctx, sql, request.AggregateID)
	if err != nil {
		return fmt.Errorf("Unable to forget snapshots: %w", err)
	}

	return nil
}

type PgSnapshot struct {
	ID               string      `db:"id"`
	AggregateID      string      `db:"aggregate_id"`
	AggregateVersion int         `db:"aggregate_version"`
	Body             common.Json `db:"body"`
	CreatedAt        time.Time   `db:"created_at"`
}
