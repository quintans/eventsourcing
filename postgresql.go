package eventstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const uniqueViolation = "23505"

var _ EventStore = (*ESPostgreSQL)(nil)

// NewESPostgreSQL creates a new instance of ESPostgreSQL
func NewESPostgreSQL(url string, snapshotThreshold int) (*ESPostgreSQL, error) {
	db, err := sqlx.Open("postgres", url)
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

func (es *ESPostgreSQL) GetByID(aggregateID string, aggregate Aggregater) error {
	snap, err := es.getSnapshot(aggregateID)
	if err != nil {
		return err
	}

	var events []PgEvent
	if snap != nil {
		events, err = es.getEvents(aggregateID, snap.AggregateVersion)
		if err != nil {
			return err
		}
		// set memento
		err = json.Unmarshal(snap.Body, aggregate)
	} else {
		events, err = es.getEvents(aggregateID, -1)
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

func (es *ESPostgreSQL) Save(aggregate Aggregater) (err error) {
	tName := nameFor(aggregate)
	version := aggregate.GetVersion()
	oldVersion := version

	tx := es.db.MustBegin()
	defer func() {
		if err != nil {
			tx.Rollback()
			aggregate.SetVersion(oldVersion)
		}
	}()
	for _, e := range aggregate.GetEvents() {
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		evt := PgEvent{
			AggregateID:      aggregate.GetID(),
			AggregateVersion: version,
			AggregateType:    tName,
			Kind:             nameFor(e),
			Body:             body,
			CreatedAt:        time.Now(),
		}

		_, err = tx.NamedExec(`
			INSERT INTO events (aggregate_id, aggregate_version, aggregate_type, kind, body, created_at)
			VALUES (:aggregate_id, :aggregate_version, :aggregate_type, :kind, :body, :created_at)
		`, &evt)
		if pgerr, ok := err.(*pq.Error); ok {
			if pgerr.Code == uniqueViolation {
				return ErrConcurrentModification
			}
		}

		if version > es.snapshotThreshold-1 &&
			version%es.snapshotThreshold == 0 {
			if err = saveSnapshot(tx, aggregate); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func nameFor(x interface{}) string {
	t := reflect.TypeOf(x)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func (es *ESPostgreSQL) GetEventsStartingAt(eventId string) ([]Event, error) {
	return nil, nil
}

func (es *ESPostgreSQL) GetEventsStartingAtFor(eventId string, agregateTypes ...string) ([]Event, error) {
	return nil, nil
}

func (es *ESPostgreSQL) getSnapshot(aggregateID string) (*PgSnapshot, error) {
	snap := &PgSnapshot{}
	if err := es.db.Get(snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1", aggregateID); err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		}
	}
	return snap, nil
}

func saveSnapshot(tx *sqlx.Tx, agg Aggregater) error {
	body, err := json.Marshal(agg)
	if err != nil {
		return err
	}

	snap := &PgSnapshot{
		AggregateID:      agg.GetID(),
		AggregateVersion: agg.GetVersion(),
		Body:             body,
		CreatedAt:        time.Now(),
	}

	_, err = tx.NamedExec(`
	INSERT INTO snapshots (aggregate_id, aggregate_version, body, created_at)
	VALUES (:aggregate_id, :aggregate_version, :body, :created_at)
	`, snap)

	return err
}

func (es *ESPostgreSQL) getEvents(aggregateID string, snapVersion int) ([]PgEvent, error) {
	query := "SELECT * FROM events e WHERE e.aggregate_id = $1"
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query += " AND e.version >= $2"
		args = append(args, snapVersion)
	}
	query += " ORDER BY e.version ASC"
	events := []PgEvent{}
	if err := es.db.Select(events, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Aggregate with Id: %s was not found: %w", aggregateID, err)
		}
		return nil, err
	}
	return events, nil
}

type PgEvent struct {
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	AggregateType    string          `db:"aggregate_type"`
	Kind             string          `db:"kind"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}

type PgSnapshot struct {
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}
