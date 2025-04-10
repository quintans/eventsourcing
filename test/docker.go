package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

func DockerCompose(t *testing.T, path, service string, env map[string]string) {
	dc, err := compose.NewDockerCompose(path)
	require.NoError(t, err, "Error creating docker compose")

	cs := dc.WithEnv(env)
	err = cs.Up(context.Background(), compose.Wait(true))
	require.NoError(t, err, "Error starting docker compose")
}
