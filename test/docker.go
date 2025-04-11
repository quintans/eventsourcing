package test

import (
	"context"
	"log"

	"github.com/testcontainers/testcontainers-go/modules/compose"
)

func DockerCompose(path, service string, env map[string]string) {
	dc, err := compose.NewDockerCompose(path)
	if err != nil {
		log.Fatalf("Error creating docker compose: %s", err)
	}

	cs := dc.WithEnv(env)
	err = cs.Up(context.Background(), compose.Wait(true))
	if err != nil {
		log.Fatalf("Error starting docker compose: %s", err)
	}
}
