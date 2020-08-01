package poller

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/eventstore/common"
)

type PgRepository struct {
	db *sqlx.DB
}

// NewPgRepository creates a new instance of ESPostgreSQL
func NewPgRepository(dburl string) (*PgRepository, error) {
	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PgRepository{
		db: db,
	}, nil
}

func (es *PgRepository) GetLastEventID(ctx context.Context) (string, error) {
	var eventID string
	safetyMargin := time.Now().Add(lag)
	if err := es.db.GetContext(ctx, &eventID, `
	SELECT * FROM events
	WHERE created_at <= $1'
	ORDER BY id DESC LIMIT 1
	`, safetyMargin); err != nil {
		if err != sql.ErrNoRows {
			return "", fmt.Errorf("Unable to get the last event ID: %w", err)
		}
	}
	return eventID, nil
}

func (es *PgRepository) GetEvents(ctx context.Context, afterEventID string, batchSize int, filter common.Filter) ([]common.Event, error) {
	safetyMargin := time.Now().Add(lag)
	args := []interface{}{afterEventID, safetyMargin}
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE id > $1 AND created_at <= $2")
	if len(filter.AggregateTypes) > 0 {
		query.WriteString(" AND (")
		first := true
		for _, v := range filter.AggregateTypes {
			if !first {
				query.WriteString(" OR ")
			}
			first = false
			args = append(args, v)
			query.WriteString(fmt.Sprintf("aggregate_type = $%d", len(args)))
		}
		query.WriteString(")")
	}
	if len(filter.Labels) > 0 {
		if len(filter.Labels) > 0 {
			first := true
			for _, v := range filter.Labels {
				k := common.Escape(v.Key)
				query.WriteString(" AND (")
				if !first {
					query.WriteString(" OR ")
				}
				first = false
				x := common.Escape(v.Value)
				query.WriteString(fmt.Sprintf(`labels  @> '{"%s": "%s"}'`, k, x))
				query.WriteString(")")
			}
		}
	}
	query.WriteString(" ORDER BY id ASC LIMIT ")
	query.WriteString(strconv.Itoa(batchSize))

	rows, err := es.queryEvents(ctx, query.String(), args)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after '%s' for filter %+v: %w", afterEventID, filter, err)
		}
	}
	return rows, nil
}

func (es *PgRepository) queryEvents(ctx context.Context, query string, args []interface{}) ([]common.Event, error) {
	rows, err := es.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	events := []common.Event{}
	for rows.Next() {
		pg := common.PgEvent{}
		err := rows.StructScan(&pg)
		if err != nil {
			return nil, fmt.Errorf("Unable to scan to struct: %w", err)
		}
		events = append(events, common.Event{
			ID:               pg.ID,
			AggregateID:      pg.AggregateID,
			AggregateVersion: pg.AggregateVersion,
			AggregateType:    pg.AggregateType,
			Kind:             pg.Kind,
			Body:             pg.Body,
			Labels:           pg.Labels,
			CreatedAt:        pg.CreatedAt,
		})
	}
	return events, nil
}
