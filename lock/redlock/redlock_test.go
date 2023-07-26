//go:build redis

package redlock_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/lock/redlock"
)

func SetupRedis(t *testing.T) string {
	ctx := context.Background()
	tcpPort := "6379"
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "redis:alpine",
		ExposedPorts: []string{tcpPort + "/tcp"},
		WaitingFor:   wait.ForListeningPort(natPort),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()))
	})

	ip, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, natPort)
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", ip, port.Port())
	time.Sleep(2 * time.Second) // hackish wait strategy

	return addr
}

func TestRedis(t *testing.T) {
	lockKey := "123"
	ctx := context.Background()
	addr := SetupRedis(t)

	pool1 := redlock.NewPool(addr)

	lock1 := pool1.NewLock(lockKey, redlock.WithExpiry(10*time.Second))
	done1, err := lock1.Lock(ctx)
	require.NoError(t, err)
	require.NotNil(t, done1, "Expected to acquire lock")

	time.Sleep(time.Second)

	pool2 := redlock.NewPool(addr)
	require.NoError(t, err)

	lock2 := pool2.NewLock(lockKey, redlock.WithExpiry(10*time.Second))
	_, err = lock2.Lock(ctx)
	require.ErrorIs(t, err, lock.ErrLockAlreadyAcquired)

	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	done2, err := lock2.Lock(ctx)
	require.NoError(t, err)
	require.NotNil(t, done2, "Expected to acquire lock")

	start := time.Now()
	pause := 3 * time.Second

	go func() {
		time.Sleep(pause)
		er := lock2.Unlock(ctx)
		require.NoError(t, er)
	}()

	err = lock1.WaitForUnlock(ctx)
	require.NoError(t, err)
	require.True(t, time.Since(start) > pause, "Waiting duration for lock was too short")

	_, err = lock1.Lock(ctx)
	require.NoError(t, err)
	err = lock1.Unlock(ctx)
	require.NoError(t, err)
}
