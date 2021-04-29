package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/log"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/faults"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
)

const resumeTokenSep = ":"

type Feed struct {
	logger        log.Logger
	config        DBConfig
	eventsTable   string
	partitions    uint32
	partitionsLow uint32
	partitionsHi  uint32
	flavour       string
}

type FeedOption func(*FeedOptions)

type FeedOptions struct {
	eventsTable   string
	partitions    uint32
	partitionsLow uint32
	partitionsHi  uint32
	flavour       string
}

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FeedOption {
	return func(p *FeedOptions) {
		if partitions <= 1 {
			return
		}
		p.partitions = partitions
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithFeedEventsCollection(eventsCollection string) FeedOption {
	return func(p *FeedOptions) {
		p.eventsTable = eventsCollection
	}
}

func WithFlavour(flavour string) FeedOption {
	return func(p *FeedOptions) {
		p.flavour = flavour
	}
}

type DBConfig struct {
	Database string
	Host     string
	Port     int
	Username string
	Password string
}

func NewFeed(logger log.Logger, config DBConfig, opts ...FeedOption) Feed {
	options := FeedOptions{
		eventsTable: "events",
		flavour:     "mariadb",
	}
	for _, o := range opts {
		o(&options)
	}

	return Feed{
		logger:        logger,
		config:        config,
		eventsTable:   options.eventsTable,
		partitions:    options.partitions,
		partitionsLow: options.partitionsLow,
		partitionsHi:  options.partitionsHi,
		flavour:       options.flavour,
	}
}

func (m Feed) Feed(ctx context.Context, sinker sink.Sinker) error {
	var lastResumePosition mysql.Position
	var lastResumeToken []byte
	err := store.ForEachResumeTokenInSinkPartitions(ctx, sinker, m.partitionsLow, m.partitionsHi, func(resumeToken []byte) error {
		p, err := parse(string(resumeToken))
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

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", m.config.Host, m.config.Port)
	cfg.User = m.config.Username
	cfg.Password = m.config.Password
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	cfg.Flavor = m.flavour
	cfg.Dump.ExecutionPath = ""
	// cfg.Dump.Where = `"id='0'"`

	cfg.IncludeTableRegex = []string{".*\\." + m.eventsTable}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return faults.Wrap(err)
	}

	var mu sync.Mutex
	var canalClosed bool
	go func() {
		<-ctx.Done()
		m.logger.Info("closing channel on context cancel...")
		mu.Lock()
		canalClosed = true
		c.Close()
		mu.Unlock()
	}()

	// TODO should be configurable
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second

	blh := &binlogHandler{
		logger:          m.logger,
		sinker:          sinker,
		lastResumeToken: lastResumeToken,
		partitions:      m.partitions,
		partitionsLow:   m.partitionsLow,
		partitionsHi:    m.partitionsHi,
		backoff:         b,
	}
	c.SetEventHandler(blh)

	return backoff.Retry(func() error {
		var err error
		if lastResumePosition.Name == "" {
			m.logger.Infof("Starting feeding (partitions: [%d-%d]) from the beginning???", m.partitionsLow, m.partitionsHi)
			err = c.Run()
		} else {
			m.logger.Infof("Starting feeding (partitions: [%d-%d]) from '%s'", m.partitionsLow, m.partitionsHi, lastResumePosition)
			err = c.RunFrom(lastResumePosition)
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return backoff.Permanent(faults.Wrap(err))
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

func parse(lastResumeToken string) (mysql.Position, error) {
	if len(lastResumeToken) == 0 {
		return mysql.Position{}, nil
	}

	s := strings.Split(string(lastResumeToken), resumeTokenSep)
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

type binlogHandler struct {
	canal.DummyEventHandler // Dummy handler from external lib
	logger                  log.Logger
	events                  []eventstore.Event
	sinker                  sink.Sinker
	lastResumeToken         []byte
	partitions              uint32
	partitionsLow           uint32
	partitionsHi            uint32
	backoff                 *backoff.ExponentialBackOff
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	if e.Action != canal.InsertAction {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			h.logger.Error(r, " ", string(debug.Stack()))
		}
	}()

	columns := e.Table.Columns

	// base value for canal.InsertAction
	for i := 0; i < len(e.Rows); i++ {
		r := rec{row: e.Rows[i], cols: columns}
		hash := r.getAsUint32("aggregate_id_hash")
		// we check the first because all the rows are for the same transaction,
		// and for the same aggregate
		if i == 0 && h.partitions > 0 {
			// check if the event is to be forwarded to the sinker
			part := common.WhichPartition(hash, h.partitions)
			if part < h.partitionsLow || part > h.partitionsHi {
				// we exit the loop because all rows are for the same aggregate
				return nil
			}
		}
		h.events = append(h.events, eventstore.Event{
			ID:               r.getAsString("id"),
			AggregateID:      r.getAsString("aggregate_id"),
			AggregateIDHash:  hash,
			AggregateVersion: r.getAsUint32("aggregate_version"),
			AggregateType:    r.getAsString("aggregate_type"),
			Kind:             r.getAsString("kind"),
			Body:             r.getAsBytes("body"),
			IdempotencyKey:   r.getAsString("idempotency_key"),
			Metadata:         r.getAsMap("metadata"),
			CreatedAt:        r.getAsTimeDate("created_at"),
		})
	}

	return nil
}

type rec struct {
	row  []interface{}
	cols []schema.TableColumn
}

func (r *rec) getAsBytes(colName string) []byte {
	return []byte(r.getAsString(colName))
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

func (r *rec) getAsMap(colName string) map[string]interface{} {
	if o := r.find(colName); o != nil {
		m := map[string]interface{}{}
		json.Unmarshal(o.([]byte), &m)
		return m
	}
	return nil
}

func (r *rec) find(colName string) interface{} {
	for k, v := range r.cols {
		if v.Name == colName {
			return r.row[k]
		}
	}
	return nil
}

func (h *binlogHandler) String() string { return "binlogHandler" }

func (h *binlogHandler) OnXID(xid mysql.Position) error {
	if len(h.events) == 0 {
		return nil
	}

	for k, event := range h.events {
		if k == len(h.events)-1 {
			// we update the resume token on the last event of the transaction
			h.lastResumeToken = format(xid)
		}
		event.ResumeToken = h.lastResumeToken
		err := h.sinker.Sink(context.Background(), event)
		if err != nil {
			return faults.Wrap(backoff.Permanent(err))
		}
	}

	h.backoff.Reset()
	h.events = nil
	return nil
}
