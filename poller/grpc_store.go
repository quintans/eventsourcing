package poller

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	pb "github.com/quintans/eventstore/api/proto"
	"github.com/quintans/eventstore/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type GrpcRepository struct {
	client pb.StoreClient
}

func NewGrpcRepository(ctx context.Context, address string) (Repository, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("did not connect: %w", err)
	}

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	c := pb.NewStoreClient(conn)
	return &GrpcRepository{
		client: c,
	}, nil
}

func (c *GrpcRepository) GetLastEventID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := c.client.GetLastEventID(ctx, &pb.GetLastEventIDRequest{})
	if err != nil {
		return "", fmt.Errorf("could not get last event id: %w", err)
	}
	return r.EventId, nil
}

func (c *GrpcRepository) GetEvents(ctx context.Context, afterEventID string, limit int, filter common.Filter) ([]common.Event, error) {
	types := make([]string, len(filter.AggregateTypes))
	for k, v := range filter.AggregateTypes {
		types[k] = v
	}
	labels := make([]*pb.Label, len(filter.Labels))
	for k, v := range filter.Labels {
		labels[k] = &pb.Label{Key: v.Key, Value: v.Value}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := c.client.GetEvents(ctx, &pb.GetEventsRequest{
		AfterEventId: afterEventID,
		Limit:        int32(limit),
		Filter: &pb.Filter{
			AggregateTypes: types,
			Labels:         labels,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("could not get events: %w", err)
	}

	events := make([]common.Event, len(r.Events))
	for k, v := range r.Events {
		createdAt, err := TimestampToTime(v.CreatedAt)
		if err != nil {
			log.Fatal("could convert timestamp")
		}

		events[k] = common.Event{
			ID:               v.Id,
			AggregateID:      v.AggregateId,
			AggregateVersion: int(v.AggregateVersion),
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             []byte(v.Body),
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           []byte(v.Labels),
			CreatedAt:        *createdAt,
		}
	}
	return events, nil

}

func TimestampToTime(ts *timestamp.Timestamp) (*time.Time, error) {
	var exp *time.Time
	if ts != nil {
		t, err := ptypes.Timestamp(ts)
		if err != nil {
			return nil, err
		}
		exp = &t
	}
	return exp, nil
}
