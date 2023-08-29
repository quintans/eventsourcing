package integration

import (
	"context"
	"testing"
	"time"

	eslog "github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/mysql"
	shared "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/worker"
	"github.com/quintans/toolkit/latch"
	"github.com/stretchr/testify/require"
)

// EventForwarderWorkerToNATS creates workers that listen to database changes,
// transform them to events and publish them into the message bus.
func EventForwarderWorker(t *testing.T, ctx context.Context, logger eslog.Logger, ltx *latch.CountDownLatch, dbConfig shared.DBConfig, sinker sink.Sinker) {
	lockExpiry := 10 * time.Second

	dbConf := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	feed, err := mysql.NewFeed(logger, dbConf, sinker)
	require.NoError(t, err)

	ltx.Add(1)
	go func() {
		<-ctx.Done()
		sinker.Close()
		ltx.Done()
	}()

	// setting nil for the locker factory means no lock will be used.
	// when we have multiple replicas/processes forwarding events to the message queue,
	// we need to use a distributed lock.
	forwarder := projection.EventForwarderWorker(logger, "account-forwarder", nil, feed.Run)
	balancer := worker.NewSingleBalancer(logger, forwarder, lockExpiry/2)
	ltx.Add(1)
	go func() {
		balancer.Start(ctx)
		<-ctx.Done()
		balancer.Stop(context.Background())
		ltx.Done()
	}()
}
