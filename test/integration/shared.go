package integration

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/mysql"
	shared "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/worker"
	"github.com/stretchr/testify/require"
)

// EventForwarderWorker creates workers that listen to database changes,
// transform them to events and publish them into the message bus.
func EventForwarderWorker(t *testing.T, ctx context.Context, logger *slog.Logger, dbConfig shared.DBConfig, sinker sink.Sinker[ulid.ULID]) {
	dbConf := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	feed, err := mysql.NewFeed(logger, dbConf, sinker)
	require.NoError(t, err)

	// setting nil for the locker factory means no lock will be used.
	// when we have multiple replicas/processes forwarding events to the message queue,
	// we need to use a distributed lock.
	forwarder := projection.EventForwarderWorker(logger, "account-forwarder", nil, feed.Run)
	worker.RunSingleBalancer(ctx, logger, forwarder, 5*time.Second)
}
