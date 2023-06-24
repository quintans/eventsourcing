//go:build consul

package consullock_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/lock/consullock"
)

func SetupConsul(ctx context.Context) (testcontainers.Container, string, error) {
	tcpPort := "8500"
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "bitnami/consul:latest",
		ExposedPorts: []string{tcpPort + "/tcp"},
		WaitingFor:   wait.ForListeningPort(natPort),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", err
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", err
	}
	consulAddr := fmt.Sprintf("%s:%s", ip, port.Port())
	time.Sleep(2 * time.Second)

	return container, consulAddr, nil
}

func TestConsul(t *testing.T) {
	lockKey := "123"
	ctx := context.Background()
	container, addr, err := SetupConsul(ctx)
	require.NoError(t, err)
	defer container.Terminate(ctx)

	pool1, err := consullock.NewPool(addr)
	require.NoError(t, err)

	lock1 := pool1.NewLock(lockKey, 10*time.Second)
	done1, err := lock1.Lock(ctx)
	require.NoError(t, err)
	require.NotNil(t, done1, "Expected to acquire lock")

	time.Sleep(time.Second)

	pool2, err := consullock.NewPool(addr)
	require.NoError(t, err)

	lock2 := pool2.NewLock(lockKey, 10*time.Second)
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
