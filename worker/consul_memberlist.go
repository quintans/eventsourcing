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

// NewConsulMemberList is a member of a list of servers managing distributed workers backed by consul.
type ConsulMemberList struct {
	client *api.Client
	prefix string
	name   string
	sID    string
}

func NewConsulMemberList(address string, family string, expiration time.Duration) (*ConsulMemberList, error) {
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
		client: client,
		prefix: family,
		name:   family + "-" + uuid.New().String(),
		sID:    sID,
	}, nil
}

func (c *ConsulMemberList) Name() string {
	return c.name
}

// List lists all servers and the workers under each of them
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
		if kvPair != nil {
			s := strings.Split(string(kvPair.Value), ",")
			members = append(members, MemberWorkers{
				Name:    v,
				Workers: s,
			})
		}
	}

	return members, nil
}

// Register registers the workers under this member
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

func (c *ConsulMemberList) Unregister(ctx context.Context) error {
	options := &api.WriteOptions{}
	options = options.WithContext(ctx)

	go func() {
		c.client.Session().Renew(c.sID, options)
	}()

	kv := &api.KVPair{
		Session: c.sID,
		Key:     c.name,
	}

	acquired, _, err := c.client.KV().DeleteCAS(kv, options)
	if err != nil {
		return faults.Wrap(err)
	}

	if !acquired {
		c.client.Session().Destroy(c.sID, options)
		return nil
	}

	return nil
}
