package mysql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

const resumeTokenSep = ":"

type Feed struct {
	logger                log.Logger
	config                DBConfig
	eventsTable           string
	partitions            uint32
	partitionsLow         uint32
	partitionsHi          uint32
	flavour               string
	backoffMaxElapsedTime time.Duration
}

type FeedOption func(*Feed)

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FeedOption {
	return func(p *Feed) {
		if partitions <= 1 {
			return
		}
		p.partitions = partitions
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithFeedEventsCollection(eventsCollection string) FeedOption {
	return func(p *Feed) {
		p.eventsTable = eventsCollection
	}
}

func WithFlavour(flavour string) FeedOption {
	return func(p *Feed) {
		p.flavour = flavour
	}
}

func WithBackoffMaxElapsedTime(duration time.Duration) FeedOption {
	return func(p *Feed) {
		p.backoffMaxElapsedTime = duration
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
	feed := Feed{
		logger:                logger,
		config:                config,
		eventsTable:           "events",
		flavour:               "mariadb",
		backoffMaxElapsedTime: 10 * time.Second,
	}
	for _, o := range opts {
		o(&feed)
	}

	return feed
}

func (f Feed) Feed(ctx context.Context, sinker sink.Sinker) error {
	var lastResumePosition mysql.Position
	var lastResumeToken []byte
	err := store.ForEachResumeTokenInSinkPartitions(ctx, sinker, f.partitionsLow, f.partitionsHi, func(message *eventsourcing.Event) error {
		p, err := parse(string(message.ResumeToken))
		if err != nil {
			return faults.Wrap(err)
		}
		if p.Compare(lastResumePosition) > 0 {
			lastResumePosition = p
			lastResumeToken = message.ResumeToken
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

	blh := &binlogHandler{
		logger:          f.logger,
		sinker:          sinker,
		lastResumeToken: lastResumeToken,
		partitions:      f.partitions,
		partitionsLow:   f.partitionsLow,
		partitionsHi:    f.partitionsHi,
		backoff:         b,
	}
	c.SetEventHandler(blh)

	return backoff.Retry(func() error {
		var err error
		if lastResumePosition.Name == "" {
			f.logger.Infof("Starting feeding (partitions: [%d-%d]) from the beginning", f.partitionsLow, f.partitionsHi)
			err = c.Run()
		} else {
			f.logger.Infof("Starting feeding (partitions: [%d-%d]) from '%s'", f.partitionsLow, f.partitionsHi, lastResumePosition)
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

func (f Feed) newCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	buf := make([]byte, 4)
	rand.Read(buf) // Always succeeds, no need to check error
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
	events                  []eventsourcing.Event
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
		id, err := eventid.Parse(r.getAsString("id"))
		if err != nil {
			return faults.Wrap(err)
		}
		h.events = append(h.events, eventsourcing.Event{
			ID:               id,
			AggregateID:      r.getAsString("aggregate_id"),
			AggregateIDHash:  hash,
			AggregateVersion: r.getAsUint32("aggregate_version"),
			AggregateType:    eventsourcing.AggregateType(r.getAsString("aggregate_type")),
			Kind:             eventsourcing.EventKind(r.getAsString("kind")),
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
