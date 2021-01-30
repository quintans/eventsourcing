package worker

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/quintans/faults"
)

var _ Memberlister = (*ConsulMemberList)(nil)

type ConsulMemberList struct {
	client     *api.Client
	prefix     string
	name       string
	expiration time.Duration
	sID        string
}

func NewConsulMemberList(address string, prefix string, expiration time.Duration) (*ConsulMemberList, error) {
	client, err := api.NewClient(&api.Config{Address: address})
	if err != nil {
		return nil, faults.Wrap(err)
	}

	sEntry := &api.SessionEntry{
		TTL:      expiration.String(),
		Behavior: "delete",
	}
	sID, _, err := client.Session().Create(sEntry, nil)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return &ConsulMemberList{
		client:     client,
		prefix:     prefix,
		name:       prefix + "-" + uuid.New().String(),
		expiration: expiration,
		sID:        sID,
	}, nil
}

func (c *ConsulMemberList) Name() string {
	return c.name
}

func (c *ConsulMemberList) List(ctx context.Context) ([]MemberWorkers, error) {
	members := []MemberWorkers{}
	options := &api.QueryOptions{}
	options = options.WithContext(ctx)
	keys, _, err := c.client.KV().Keys(c.prefix+"-", "", options)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	for _, v := range keys {
		kvPair, _, err := c.client.KV().Get(v, options)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		s := strings.Split(string(kvPair.Value), ",")
		members = append(members, MemberWorkers{
			Name:    v,
			Workers: s,
		})
	}

	return members, nil
}

func (c *ConsulMemberList) Register(ctx context.Context, workers []string) error {
	options := &api.WriteOptions{}
	options = options.WithContext(ctx)

	go func() {
		c.client.Session().Renew(c.sID, options)
	}()

	s := strings.Join(workers, ",")
	kv := &api.KVPair{
		Session: c.sID,
		Key:     c.name,
		Value:   []byte(s),
	}

	acquired, _, err := c.client.KV().Acquire(kv, options)
	if err != nil {
		return faults.Wrap(err)
	}

	if !acquired {
		c.client.Session().Destroy(c.sID, options)
		return nil
	}

	return nil
}
