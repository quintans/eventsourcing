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
	address string
}

func NewGrpcRepository(address string) Repository {
	return GrpcRepository{
		address: address,
	}
}

func (c GrpcRepository) GetLastEventID(ctx context.Context, filter common.Filter) (string, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := cli.GetLastEventID(ctx, &pb.GetLastEventIDRequest{})
	if err != nil {
		return "", fmt.Errorf("could not get last event id: %w", err)
	}
	return r.EventId, nil
}

func (c GrpcRepository) GetEvents(ctx context.Context, afterEventID string, limit int, filter common.Filter) ([]common.Event, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	pbFilter := filterToPbFilter(filter)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := cli.GetEvents(ctx, &pb.GetEventsRequest{
		AfterEventId: afterEventID,
		Limit:        int32(limit),
		Filter:       pbFilter,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get events: %w", err)
	}

	events := make([]common.Event, len(r.Events))
	for k, v := range r.Events {
		createdAt, err := tsToTime(v.CreatedAt)
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

func filterToPbFilter(filter common.Filter) *pb.Filter {
	types := make([]string, len(filter.AggregateTypes))
	for k, v := range filter.AggregateTypes {
		types[k] = v
	}
	labels := make([]*pb.Label, len(filter.Labels))
	for k, v := range filter.Labels {
		labels[k] = &pb.Label{Key: v.Key, Value: v.Value}
	}
	return &pb.Filter{
		AggregateTypes: types,
		Labels:         labels,
	}
}

func (c GrpcRepository) dial() (pb.StoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("did not connect: %w", err)
	}
	return pb.NewStoreClient(conn), conn, nil
}

func tsToTime(ts *timestamp.Timestamp) (*time.Time, error) {
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
