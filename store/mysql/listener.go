package mysql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
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
	sinker                sink.Sinker
	setSeqRepo            SetSeqRepository
}

type SetSeqRepository interface {
	SetSinkData(ctx context.Context, eID eventid.EventID, data sink.Data) error
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

func NewFeed(logger log.Logger, config DBConfig, sinker sink.Sinker, setSeqRepo SetSeqRepository, opts ...FeedOption) (*Feed, error) {
	feed := &Feed{
		logger:                logger,
		config:                config,
		eventsTable:           "events",
		flavour:               "mariadb",
		backoffMaxElapsedTime: 10 * time.Second,
		sinker:                sinker,
		setSeqRepo:            setSeqRepo,
	}
	for _, o := range opts {
		o(feed)
	}

	if feed.partitions < 1 {
		return nil, faults.Errorf("the number of partitions (%d) must be greater than than 0", feed.partitions)
	}
	if feed.partitionsLow < 1 {
		return nil, faults.Errorf("the the partitions low bound (%d) must be greater than than 0", feed.partitionsLow)
	}
	if feed.partitionsHi < 1 {
		return nil, faults.Errorf("the the partitions high bound (%d) must be greater than than 0", feed.partitionsHi)
	}
	if feed.partitionsHi < feed.partitionsLow {
		return nil, faults.Errorf("the the partitions high bound (%d) must be greater or equal than partitions low bound (%d) ", feed.partitionsHi, feed.partitionsLow)
	}

	return feed, nil
}

func (f *Feed) Run(ctx context.Context) error {
	f.logger.Infof("Starting Feed for '%s'", f.eventsTable)
	var lastResumePosition mysql.Position
	var lastResumeToken []byte
	err := store.ForEachSequenceInSinkPartitions(ctx, f.sinker, f.partitionsLow, f.partitionsHi, func(_ uint64, message *sink.Message) error {
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
		sinker:          f.sinker,
		lastResumeToken: lastResumeToken,
		partitions:      f.partitions,
		partitionsLow:   f.partitionsLow,
		partitionsHi:    f.partitionsHi,
		backoff:         b,
		chanel:          c,
		eventsTable:     f.eventsTable,
		setSeqRepo:      f.setSeqRepo,
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

func (f *Feed) newCanal() (*canal.Canal, error) {
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

type binlogHandler struct {
	canal.DummyEventHandler // Dummy handler from external lib
	logger                  log.Logger
	events                  []*eventsourcing.Event
	sinker                  sink.Sinker
	lastResumeToken         []byte
	partitions              uint32
	partitionsLow           uint32
	partitionsHi            uint32
	backoff                 *backoff.ExponentialBackOff
	chanel                  *canal.Canal
	eventsTable             string
	setSeqRepo              SetSeqRepository
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
		if i == 0 {
			// check if the event is to be forwarded to the sinker
			part, err := util.WhichPartition(hash, h.partitions)
			if err != nil {
				return faults.Wrap(err)
			}
			if part < h.partitionsLow || part > h.partitionsHi {
				// we exit the loop because all rows are for the same aggregate
				return nil
			}
		}
		id, err := eventid.Parse(r.getAsString("id"))
		if err != nil {
			return faults.Wrap(err)
		}
		// Partition and Sequence don't need to be assigned because at this moment they have a zero value.
		// They will be populate with the values returned by the sink.
		h.events = append(h.events, &eventsourcing.Event{
			ID:               id,
			AggregateID:      r.getAsString("aggregate_id"),
			AggregateIDHash:  hash,
			AggregateVersion: r.getAsUint32("aggregate_version"),
			AggregateKind:    eventsourcing.Kind(r.getAsString("aggregate_kind")),
			Kind:             eventsourcing.Kind(r.getAsString("kind")),
			Body:             r.getStringAsBytes("body"),
			IdempotencyKey:   r.getAsString("idempotency_key"),
			Metadata:         encoding.JSONOfBytes(r.getAsBytes("metadata")),
			CreatedAt:        r.getAsTimeDate("created_at"),
			Migrated:         r.getAsBool("migrated"),
		})
	}

	return nil
}

type rec struct {
	row  []interface{}
	cols []schema.TableColumn
}

func (r *rec) getAsBytes(colName string) []byte {
	if o := r.find(colName); o != nil {
		return o.([]byte)
	}
	return nil
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

func (h *binlogHandler) String() string { return "binlogHandler" }

func (h *binlogHandler) OnXID(header *replication.EventHeader, xid mysql.Position) error {
	if len(h.events) == 0 {
		return nil
	}

	for k, event := range h.events {
		if k == len(h.events)-1 {
			// we update the resume token on the last event of the transaction
			h.lastResumeToken = format(xid)
		}
		data, err := h.sinker.Sink(context.Background(), event, sink.Meta{ResumeToken: h.lastResumeToken})
		if err != nil {
			return faults.Wrap(backoff.Permanent(err))
		}
		err = h.setSeqRepo.SetSinkData(context.Background(), event.ID, data)
		if err != nil {
			return faults.Wrap(backoff.Permanent(err))
		}
	}

	h.backoff.Reset()
	h.events = nil
	return nil
}
