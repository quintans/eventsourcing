package player

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/quintans/eventstore"
	pb "github.com/quintans/eventstore/api/proto"
	"github.com/quintans/eventstore/repo"
	"google.golang.org/grpc"
)

type GrpcRepository struct {
	address string
	factory eventstore.Factory
	decoder eventstore.Decoder
}

func NewGrpcRepository(address string, factory eventstore.Factory) Repository {
	return GrpcRepository{
		address: address,
		factory: factory,
		decoder: eventstore.JsonCodec{},
	}
}

func (c *GrpcRepository) SetDecoder(decoder eventstore.Decoder) {
	c.decoder = decoder
}

func (c GrpcRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter repo.Filter) (string, error) {
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
		return "", fmt.Errorf("could not get last event id: %w", err)
	}
	return r.EventId, nil
}

func (c GrpcRepository) GetEvents(ctx context.Context, afterEventID string, limit int, trailingLag time.Duration, filter repo.Filter) ([]eventstore.Event, error) {
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
		TrailingLag:  trailingLag.Milliseconds(),
		Filter:       pbFilter,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get events: %w", err)
	}

	events := make([]eventstore.Event, len(r.Events))
	for k, v := range r.Events {
		createdAt, err := tsToTime(v.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("could convert timestamp to time: %w", err)
		}
		labels := map[string]interface{}{}
		err = json.Unmarshal([]byte(v.Labels), &labels)
		if err != nil {
			return nil, fmt.Errorf("Unable unmarshal labels to map: %w", err)
		}
		events[k] = eventstore.Event{
			ID:               v.Id,
			AggregateID:      v.AggregateId,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           labels,
			CreatedAt:        *createdAt,
			Decode:           eventstore.EventDecoder(c.factory, c.decoder, v.Kind, v.Body),
		}
	}
	return events, nil
}

func filterToPbFilter(filter repo.Filter) *pb.Filter {
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
