package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// natsContainer represents the nats container type used in the module
type natsContainer struct {
	testcontainers.Container
	URI string
}

// runContainer creates an instance of the nats container type
func runNatsContainer(t *testing.T) string {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "nats:2.9",
		ExposedPorts: []string{"4222/tcp", "6222/tcp", "8222/tcp"},
		Cmd:          []string{"-DV", "-js"},
		WaitingFor:   wait.ForLog("Listening for client connections on 0.0.0.0:4222"),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}
	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()), "failed to terminate container")
	})

	mappedPort, err := container.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)

	hostIP, err := container.Host(ctx)
	require.NoError(t, err)

	return fmt.Sprintf("nats://%s:%s", hostIP, mappedPort.Port())
}
