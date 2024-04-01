package worker

import (
	"context"
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-multierror"
	"github.com/quintans/faults"
	"github.com/teris-io/shortid"
)

var _ Ledger = (*ConsulLedger)(nil)

// ConsulLedger is a member of a list of servers/ledgers managing distributed workers backed by consul.
type ConsulLedger struct {
	client *api.Client
	prefix string
	name   string
	sID    string
}

func NewConsulLedger(address, family string, expiration time.Duration) (*ConsulLedger, error) {
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

	return &ConsulLedger{
		client: client,
		prefix: family,
		name:   family + "-" + shortid.MustGenerate(),
		sID:    sID,
	}, nil
}

func (c *ConsulLedger) Name() string {
	return c.name
}

// Peers lists all servers and the workers under each of them
func (c *ConsulLedger) Peers(ctx context.Context) ([]Peer, error) {
	peersWorkers := []Peer{}
	options := &api.QueryOptions{}
	options = options.WithContext(ctx)
	peers, _, err := c.client.KV().Keys(c.prefix+"-", "", options)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	type result struct {
		MemberID string
		KVPair   *api.KVPair
		Err      error
	}
	var allErr error
	ch := make(chan result, len(peers))
	for _, peerID := range peers {
		go func(m string) {
			r := result{
				MemberID: m,
			}
			retry.Do(
				func() error {
					r.KVPair, _, r.Err = c.client.KV().Get(m, options)
					return r.Err
				},
				retry.Attempts(retries),
			)
			r.Err = faults.Wrap(r.Err)
			ch <- r
		}(peerID)
		for range peers {
			r := <-ch
			if r.Err != nil {
				allErr = multierror.Append(err, r.Err)
			} else if r.KVPair != nil {
				s := strings.Split(string(r.KVPair.Value), ",")
				peersWorkers = append(peersWorkers, Peer{
					Name:    r.MemberID,
					Workers: s,
				})
			}
		}
	}
	if allErr != nil {
		return nil, allErr
	}

	return peersWorkers, nil
}

// Register registers the workers under this ledger
func (c *ConsulLedger) Register(ctx context.Context, workers []string) error {
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

func (c *ConsulLedger) Close(ctx context.Context) error {
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
