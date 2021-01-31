package player

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/quintans/eventstore"
	pb "github.com/quintans/eventstore/api/proto"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/faults"
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

func (c GrpcRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (string, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pbFilter := filterToPbFilter(filter)
	r, err := cli.GetLastEventID(ctx, &pb.GetLastEventIDRequest{
		TrailingLag: trailingLag.Milliseconds(),
		Filter:      pbFilter,
	})
	if err != nil {
		return "", faults.Errorf("could not get last event id: %w", err)
	}
	return r.EventId, nil
}

func (c GrpcRepository) GetEvents(ctx context.Context, afterEventID string, limit int, trailingLag time.Duration, filter store.Filter) ([]eventstore.Event, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return nil, faults.Wrap(err)
	}
	defer conn.Close()

	pbFilter := filterToPbFilter(filter)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := cli.GetEvents(ctx, &pb.GetEventsRequest{
		AfterEventId: afterEventID,
		Limit:        int32(limit),
		TrailingLag:  trailingLag.Milliseconds(),
		Filter:       pbFilter,
	})
	if err != nil {
		return nil, faults.Errorf("could not get events: %w", err)
	}

	events := make([]eventstore.Event, len(r.Events))
	for k, v := range r.Events {
		createdAt, err := tsToTime(v.CreatedAt)
		if err != nil {
			return nil, faults.Errorf("could convert timestamp to time: %w", err)
		}
		labels := map[string]interface{}{}
		err = json.Unmarshal([]byte(v.Labels), &labels)
		if err != nil {
			return nil, faults.Errorf("Unable unmarshal labels to map: %w", err)
		}
		events[k] = eventstore.Event{
			ID:               v.Id,
			AggregateID:      v.AggregateId,
			AggregateIDHash:  v.AggregateIdHash,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           labels,
			CreatedAt:        *createdAt,
		}
	}
	return events, nil
}

func filterToPbFilter(filter store.Filter) *pb.Filter {
	types := make([]string, len(filter.AggregateTypes))
	for k, v := range filter.AggregateTypes {
		types[k] = v
	}
	labels := []*pb.Label{}
	for key, v := range filter.Labels {
		for _, value := range v {
			labels = append(labels, &pb.Label{Key: key, Value: value})
		}
	}
	return &pb.Filter{
		AggregateTypes: types,
		Labels:         labels,
		Partitions:     filter.Partitions,
		PartitionLow:   filter.PartitionLow,
		PartitionHi:    filter.PartitionHi,
	}
}

func (c GrpcRepository) dial() (pb.StoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, faults.Errorf("did not connect: %w", err)
	}
	return pb.NewStoreClient(conn), conn, nil
}

func tsToTime(ts *timestamp.Timestamp) (*time.Time, error) {
	var exp *time.Time
	if ts != nil {
		t, err := ptypes.Timestamp(ts)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		exp = &t
	}
	return exp, nil
}
