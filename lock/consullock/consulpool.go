package consullock

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

type Pool struct {
	client *api.Client
}

func NewPool(consulAddress string) (Pool, error) {
	api.DefaultConfig()
	client, err := api.NewClient(&api.Config{Address: consulAddress})
	if err != nil {
		return Pool{}, err
	}

	if err != nil {
		return Pool{}, fmt.Errorf("session create err: %w", err)
	}

	return Pool{
		client: client,
	}, nil
}

func (p Pool) NewLock(lockName string, expiry time.Duration) *Lock {
	return &Lock{
		client:   p.client,
		lockName: lockName,
		expiry:   expiry,
	}
}
