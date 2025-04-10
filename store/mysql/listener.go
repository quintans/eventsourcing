package mysql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const resumeTokenSep = ":"

type Feed[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	logger                *slog.Logger
	config                DBConfig
	eventsTable           string
	flavour               string
	backoffMaxElapsedTime time.Duration
	sinker                sink.Sinker[K]
	filter                *store.Filter
}

type FeedOption[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*Feed[K, PK])

func WithFeedEventsTable[K eventsourcing.ID, PK eventsourcing.IDPt[K]](eventsCollection string) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.eventsTable = eventsCollection
	}
}

func WithFlavour[K eventsourcing.ID, PK eventsourcing.IDPt[K]](flavour string) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.flavour = flavour
	}
}

func WithBackoffMaxElapsedTime[K eventsourcing.ID, PK eventsourcing.IDPt[K]](duration time.Duration) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.backoffMaxElapsedTime = duration
	}
}

func WithFilter[K eventsourcing.ID, PK eventsourcing.IDPt[K]](filter *store.Filter) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.filter = filter
	}
}

type DBConfig struct {
	Database string
	Host     string
	Port     int
	Username string
	Password string
}

func NewFeed[K eventsourcing.ID, PK eventsourcing.IDPt[K]](logger *slog.Logger, config DBConfig, sinker sink.Sinker[K], opts ...FeedOption[K, PK]) (*Feed[K, PK], error) {
	feed := &Feed[K, PK]{
		logger:                logger.With("feed", "mysql"),
		config:                config,
		eventsTable:           "events",
		flavour:               "mariadb",
		backoffMaxElapsedTime: 10 * time.Second,
		sinker:                sinker,
	}
	for _, o := range opts {
		o(feed)
	}

	return feed, nil
}

func (f *Feed[K, PK]) Run(ctx context.Context) error {
	f.logger.Info("Starting Feed", "eventsTable", f.eventsTable)
	var lastResumePosition mysql.Position
	var lastResumeToken []byte
	err := f.sinker.ResumeTokens(ctx, func(resumeToken encoding.Base64) error {
		p, err := parse(resumeToken.AsString())
		if err != nil {
			return faults.Wrap(err)
		}
		if p.Compare(lastResumePosition) > 0 {
			lastResumePosition = p
			lastResumeToken = resumeToken
		}
		return nil
	})
	if err != nil {
		return err
	}

	c, err := f.newCanal()
	if err != nil {
		return err
	}

	var mu sync.Mutex
	var canalClosed bool
	go func() {
		<-ctx.Done()
		f.logger.Info("closing channel on context cancel...")
		mu.Lock()
		canalClosed = true
		c.Close()
		mu.Unlock()
	}()

	// TODO should be configurable
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = f.backoffMaxElapsedTime

	blh := &binlogHandler[K, PK]{
		logger:          f.logger,
		sinker:          f.sinker,
		lastResumeToken: lastResumeToken,
		backoff:         b,
		chanel:          c,
		eventsTable:     f.eventsTable,
		filter:          f.filter,
	}
	c.SetEventHandler(blh)

	return backoff.Retry(func() error {
		var err error
		if lastResumePosition.Name == "" {
			f.logger.Info("Starting feeding from the beginning")
			err = c.Run()
		} else {
			f.logger.Info("Starting feeding",
				"from", lastResumePosition)
			err = c.RunFrom(lastResumePosition)
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			mu.Lock()
			if canalClosed {
				mu.Unlock()
				return nil
			}
			mu.Unlock()

			return faults.Wrap(err)
		}
		return nil
	}, b)
}

func (f *Feed[K, PK]) newCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	buf := make([]byte, 4)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	cfg.ServerID = binary.LittleEndian.Uint32(buf)

	cfg.Addr = fmt.Sprintf("%s:%d", f.config.Host, f.config.Port)
	cfg.User = f.config.Username
	cfg.Password = f.config.Password
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	cfg.Flavor = f.flavour
	cfg.Dump.ExecutionPath = ""
	// cfg.Dump.Where = `"id='0'"`

	cfg.IncludeTableRegex = []string{".*\\." + f.eventsTable}

	c, err := canal.NewCanal(cfg)
	return c, faults.Wrap(err)
}

func parse(lastResumeToken string) (mysql.Position, error) {
	if lastResumeToken == "" {
		return mysql.Position{}, nil
	}

	s := strings.Split(lastResumeToken, resumeTokenSep)
	pos, err := strconv.ParseUint(s[1], 10, 32)
	if err != nil {
		return mysql.Position{}, faults.Errorf("unable to parse '%s' as uint32: %w", s[1], err)
	}
	return mysql.Position{
		Name: s[0],
		Pos:  uint32(pos),
	}, nil
}

func format(xid mysql.Position) []byte {
	if xid.Name == "" {
		return []byte{}
	}
	return []byte(xid.Name + resumeTokenSep + strconv.FormatInt(int64(xid.Pos), 10))
}

type binlogHandler[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	canal.DummyEventHandler // Dummy handler from external lib
	logger                  *slog.Logger
	events                  []*eventsourcing.Event[K]
	sinker                  sink.Sinker[K]
	lastResumeToken         []byte
	backoff                 *backoff.ExponentialBackOff
	chanel                  *canal.Canal
	eventsTable             string
	filter                  *store.Filter
}

func (h *binlogHandler[K, PK]) OnRow(e *canal.RowsEvent) error {
	if e.Action != canal.InsertAction {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("panic", "recover", r, "stack", string(debug.Stack()))
		}
	}()

	columns := e.Table.Columns

	// base value for canal.InsertAction
	for i := 0; i < len(e.Rows); i++ {
		r := rec{row: e.Rows[i], cols: columns}
		id, err := eventid.Parse(r.getAsString("id"))
		if err != nil {
			return faults.Wrap(err)
		}

		aggID := PK(new(K))
		aggIDStr := r.getAsString("aggregate_id")
		err = aggID.UnmarshalText([]byte(aggIDStr))
		if err != nil {
			return faults.Errorf("unmarshaling id '%s': %w", aggIDStr, err)
		}
		// discriminator
		disc := eventsourcing.Discriminator{}
		for k, v := range r.cols {
			if r.row[k] == nil || !strings.HasPrefix(v.Name, store.DiscriminatorColumnPrefix) {
				continue
			}

			c, ok := r.row[k].(string)
			if !ok {
				return faults.Errorf("discriminator for column '%s' must be a string, got %T", v.Name, r.row[k])
			}
			disc[v.Name[len(store.DiscriminatorColumnPrefix):]] = c
		}
		event := &eventsourcing.Event[K]{
			ID:               id,
			AggregateID:      *aggID,
			AggregateIDHash:  r.getAsUint32("aggregate_id_hash"),
			AggregateVersion: r.getAsUint32("aggregate_version"),
			AggregateKind:    eventsourcing.Kind(r.getAsString("aggregate_kind")),
			Kind:             eventsourcing.Kind(r.getAsString("kind")),
			Body:             r.getStringAsBytes("body"),
			Discriminator:    disc,
			CreatedAt:        r.getAsTimeDate("created_at"),
			Migrated:         r.getAsBool("migrated"),
		}
		// we check the first because all the rows are for the same transaction,
		// therefore for the same aggregate
		if i == 0 {
			// check if the event is to be forwarded to the sinker
			if !h.accepts(event) {
				// we exit the loop because all rows are for the same aggregate
				return nil
			}
		}
		h.events = append(h.events, event)
	}

	return nil
}

// accepts check if the event is to be forwarded to the sinker
func (h *binlogHandler[K, PK]) accepts(event *eventsourcing.Event[K]) bool {
	if h.filter == nil {
		return true
	}

	if h.filter.Splits > 1 && len(h.filter.SplitIDs) != int(h.filter.Splits) {
		p := util.CalcPartition(event.AggregateIDHash, h.filter.Splits)
		if !slices.Contains(h.filter.SplitIDs, p) {
			return false
		}
	}

	if len(h.filter.AggregateKinds) > 0 && !slices.Contains(h.filter.AggregateKinds, event.AggregateKind) {
		return false
	}

	for _, v := range h.filter.Discriminator {
		val := event.Discriminator[v.Key]
		if val == "" || !slices.Contains(v.Values, val) {
			return false
		}
	}

	return true
}

type rec struct {
	row  []interface{}
	cols []schema.TableColumn
}

func (r *rec) getStringAsBytes(colName string) []byte {
	s := r.getAsString(colName)
	if s == "" {
		return nil
	}
	return []byte(s)
}

func (r *rec) getAsString(colName string) string {
	if o := r.find(colName); o != nil {
		return o.(string)
	}
	return ""
}

func (r *rec) getAsTimeDate(colName string) time.Time {
	if o := r.find(colName); o != nil {
		t, _ := time.Parse("2006-01-02 15:04:05", o.(string))

		return t
	}
	return time.Time{}
}

func (r *rec) getAsUint32(colName string) uint32 {
	if o := r.find(colName); o != nil {
		return uint32(o.(int32))
	}
	return 0
}

func (r *rec) getAsBool(colName string) bool {
	if o := r.find(colName); o != nil {
		return o.(int8) == 1
	}
	return false
}

func (r *rec) find(colName string) interface{} {
	for k := range r.cols {
		v := r.cols[k]
		if v.Name == colName {
			return r.row[k]
		}
	}
	return nil
}

func (h *binlogHandler[K, PK]) String() string { return "binlogHandler" }

func (h *binlogHandler[K, PK]) OnXID(header *replication.EventHeader, xid mysql.Position) error {
	if len(h.events) == 0 {
		return nil
	}

	for k, event := range h.events {
		if k == len(h.events)-1 {
			// we update the resume token on the last event of the transaction
			h.lastResumeToken = format(xid)
		}
		err := h.sinker.Sink(context.Background(), event, sink.Meta{ResumeToken: h.lastResumeToken})
		if err != nil {
			return faults.Wrap(backoff.Permanent(err))
		}
	}

	h.backoff.Reset()
	h.events = nil
	return nil
}
