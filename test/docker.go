package test

import (
	"log"
	"testing"
	"time"

	"github.com/quintans/faults"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func DockerCompose(t *testing.T, path, service string, wait time.Duration) {
	compose := testcontainers.NewLocalDockerCompose([]string{path}, service+"-set")

	t.Cleanup(func() {
		exErr := compose.Down()
		if err := checkIfError(exErr); err != nil {
			log.Printf("Error on compose shutdown: %v\n", err)
		}
	})

	exErr := compose.Down()
	require.NoError(t, checkIfError(exErr), "Error on compose shutdown")

	exErr = compose.WithCommand([]string{"up", "-d"}).
		Invoke()
	require.NoError(t, checkIfError(exErr))

	time.Sleep(wait)
}

func checkIfError(err testcontainers.ExecError) error {
	if err.Error != nil {
		return faults.Errorf("Failed when running %v: %v", err.Command, err.Error)
	}

	if err.Stdout != nil {
		return faults.Errorf("An error in Stdout happened when running %v: %v", err.Command, err.Stdout)
	}

	if err.Stderr != nil {
		return faults.Errorf("An error in Stderr happened when running %v: %v", err.Command, err.Stderr)
	}
	return nil
}
