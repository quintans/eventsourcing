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
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

const uniqueViolation = "23505"

var _ EventStore = (*ESPostgreSQL)(nil)
var _ Tracker = (*ESPostgreSQL)(nil)

// NewESPostgreSQL creates a new instance of ESPostgreSQL
func NewESPostgreSQL(dburl string, snapshotThreshold int) (*ESPostgreSQL, error) {
	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &ESPostgreSQL{
		db:                db,
		snapshotThreshold: snapshotThreshold,
	}, nil
}

// ESPostgreSQL is the implementation of an Event Store in PostgreSQL
type ESPostgreSQL struct {
	db                *sqlx.DB
	snapshotThreshold int
}

func (es *ESPostgreSQL) GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error {
	snap, err := es.getSnapshot(ctx, aggregateID)
	if err != nil {
		return err
	}

	var events []PgEvent
	if snap != nil {
		err = json.Unmarshal(snap.Body, aggregate)
		if err != nil {
			return err
		}
		events, err = es.getEvents(ctx, aggregateID, snap.AggregateVersion)
	} else {
		events, err = es.getEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return err
	}

	for _, v := range events {
		aggregate.ApplyChangeFromHistory(Event{
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

func (es *ESPostgreSQL) Save(ctx context.Context, aggregate Aggregater) (err error) {
	tName := nameFor(aggregate)
	version := aggregate.GetVersion()
	oldVersion := version

	var eventID string
	var takeSnapshot bool
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
			aggregate.SetVersion(oldVersion)
			return
		}
		if takeSnapshot {
			snap, err := buildSnapshot(aggregate, eventID)
			if err != nil {
				go es.saveSnapshot(ctx, snap)
			}
		}
	}()

	for _, e := range aggregate.GetEvents() {
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}

		eventID := xid.New().String()
		_, err = tx.ExecContext(ctx,
			`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			eventID, aggregate.GetID(), version, tName, nameFor(e), body)
		if err != nil {
			if pgerr, ok := err.(*pq.Error); ok {
				if pgerr.Code == uniqueViolation {
					return ErrConcurrentModification
				}
			}
			return fmt.Errorf("Unable to retrieve event ID: %w", err)
		}

		if version > es.snapshotThreshold-1 &&
			version%es.snapshotThreshold == 0 {
			takeSnapshot = true
		}

	}
	aggregate.ClearEvents()
	return tx.Commit()
}

func nameFor(x interface{}) string {
	t := reflect.TypeOf(x)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func (es *ESPostgreSQL) GetLastEventID(ctx context.Context) (string, error) {
	var eventID string
	if err := es.db.GetContext(ctx, &eventID, `
	SELECT * FROM events
	WHERE id > $1 AND created_at <= NOW()::TIMESTAMP - INTERVAL'1 seconds'
	ORDER BY id DESC LIMIT 1
	`); err != nil {
		if err != sql.ErrNoRows {
			return "", fmt.Errorf("Unable to get the last event ID: %w", err)
		}
	}
	return eventID, nil
}

func (es *ESPostgreSQL) GetEventsForAggregate(ctx context.Context, afterEventID string, aggregateID string, limit int) ([]Event, error) {
	events := []Event{}
	if err := es.db.SelectContext(ctx, &events, `
	SELECT * FROM events
	WHERE id > $1
	AND aggregate_id = $2
	AND created_at <= NOW()::TIMESTAMP - INTERVAL'1 seconds'
	ORDER BY id ASC LIMIT $3
	`, afterEventID, aggregateID, limit); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after %q: %w", afterEventID, err)
		}
	}
	return events, nil

}

func (es *ESPostgreSQL) GetEvents(ctx context.Context, afterEventID string, limit int, aggregateTypes ...string) ([]Event, error) {
	args := []interface{}{afterEventID}
	query := `SELECT * FROM events
	WHERE id > $1
	AND created_at <= NOW()::TIMESTAMP - INTERVAL'1 seconds'`
	if len(aggregateTypes) > 0 {
		query += " AND aggregate_type IN ($2)"
		args = append(args, aggregateTypes)
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args))
	log.Println("===>", query)

	rows, err := es.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after %q for %s: %w", afterEventID, aggregateTypes, err)
		}
	}
	events := []Event{}
	for rows.Next() {
		pg := PgEvent{}
		err := rows.StructScan(&pg)
		if err != nil {
			return nil, fmt.Errorf("Unable to scan to struct: %w", err)
		}
		events = append(events, Event{
			ID:               pg.ID,
			AggregateID:      pg.AggregateID,
			AggregateVersion: pg.AggregateVersion,
			AggregateType:    pg.AggregateType,
			Kind:             pg.Kind,
			Body:             pg.Body,
			CreatedAt:        pg.CreatedAt,
		})
	}
	return events, nil
}

func (es *ESPostgreSQL) getSnapshot(ctx context.Context, aggregateID string) (*PgSnapshot, error) {
	snap := &PgSnapshot{}
	if err := es.db.GetContext(ctx, snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("Unable to get snapshot for aggregate %q: %w", aggregateID, err)
	}
	return snap, nil
}

func (es *ESPostgreSQL) saveSnapshot(ctx context.Context, snap interface{}) {
	_, err := es.db.NamedExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, body, created_at)
	     VALUES (:id, :aggregate_id, :aggregate_version, :body, :created_at)`, snap)

	if err != nil {
		log.WithField("snapshot", snap).
			WithError(err).
			Error("Failed to save snapshot")
	}
}

func buildSnapshot(agg Aggregater, eventID string) (*PgSnapshot, error) {
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
		CreatedAt:        time.Now(),
	}, nil
}

func (es *ESPostgreSQL) getEvents(ctx context.Context, aggregateID string, snapVersion int) ([]PgEvent, error) {
	query := "SELECT * FROM events e WHERE e.aggregate_id = $1"
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query += " AND e.aggregate_version > $2"
		args = append(args, snapVersion)
	}
	events := []PgEvent{}
	if err := es.db.SelectContext(ctx, &events, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Aggregate %q was not found: %w", aggregateID, err)
		}
		return nil, fmt.Errorf("Unable to get events for Aggregate %q: %w", aggregateID, err)
	}
	return events, nil
}

type PgEvent struct {
	ID               string          `db:"id"`
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	AggregateType    string          `db:"aggregate_type"`
	Kind             string          `db:"kind"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}

type PgSnapshot struct {
	ID               string          `db:"id"`
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}
