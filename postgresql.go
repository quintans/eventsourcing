package eventstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

const uniqueViolation = "23505"

var _ EventStore = (*ESPostgreSQL)(nil)

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

	snapshots := []interface{}{}
	tx := es.db.MustBegin()
	defer func() {
		if err != nil {
			tx.Rollback()
			aggregate.SetVersion(oldVersion)
			return
		}
		for _, s := range snapshots {
			go es.saveSnapshot(s)
		}
	}()
	for _, e := range aggregate.GetEvents() {
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}

		var eventID int64
		err = tx.Get(&eventID,
			`INSERT INTO events (aggregate_id, aggregate_version, aggregate_type, kind, body)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			aggregate.GetID(), version, tName, nameFor(e), body)
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
			snap, err := buildSnapshot(aggregate, eventID)
			if err == nil {
				snapshots = append(snapshots, snap)
			}
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

func (es *ESPostgreSQL) GetEventsStartingAt(eventId string) ([]Event, error) {
	return nil, nil
}

func (es *ESPostgreSQL) GetEventsStartingAtFor(eventId string, agregateTypes ...string) ([]Event, error) {
	return nil, nil
}

func (es *ESPostgreSQL) getSnapshot(aggregateID string) (*PgSnapshot, error) {
	snap := &PgSnapshot{}
	if err := es.db.Get(snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get snapshot for aggregate %q: %w", aggregateID, err)
		}
	}
	return snap, nil
}

func (es *ESPostgreSQL) saveSnapshot(snap interface{}) {
	_, err := es.db.NamedExec(`
	INSERT INTO snapshots (id, aggregate_id, aggregate_version, body, created_at)
	VALUES (:id, :aggregate_id, :aggregate_version, :body, :created_at)
	`, snap)

	if err != nil {
		log.WithField("snapshot", snap).
			WithError(err).
			Error("Failed to save snapshot")
	}
}

func buildSnapshot(agg Aggregater, eventID int64) (interface{}, error) {
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

func (es *ESPostgreSQL) getEvents(aggregateID string, snapVersion int) ([]PgEvent, error) {
	query := "SELECT * FROM events e WHERE e.aggregate_id = $1"
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query += " AND e.aggregate_version >= $2"
		args = append(args, snapVersion)
	}
	events := []PgEvent{}
	if err := es.db.Select(&events, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Aggregate %q was not found: %w", aggregateID, err)
		}
		return nil, fmt.Errorf("Unable to get events for Aggregate %q: %w", aggregateID, err)
	}
	return events, nil
}

type PgEvent struct {
	ID               int64           `db:"id"`
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	AggregateType    string          `db:"aggregate_type"`
	Kind             string          `db:"kind"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}

type PgSnapshot struct {
	ID               int64           `db:"id"`
	AggregateID      string          `db:"aggregate_id"`
	AggregateVersion int             `db:"aggregate_version"`
	Body             json.RawMessage `db:"body"`
	CreatedAt        time.Time       `db:"created_at"`
}
