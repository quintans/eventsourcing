package projection

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/quintans/faults"
	"google.golang.org/grpc"

	"github.com/quintans/eventsourcing"
	pb "github.com/quintans/eventsourcing/api/proto"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

type GrpcRepository struct {
	address string
}

var _ EventsRepository = (*GrpcRepository)(nil)

func NewGrpcRepository(address string) GrpcRepository {
	return GrpcRepository{
		address: address,
	}
}

func (c GrpcRepository) GetMaxSeq(ctx context.Context, filter store.Filter) (uint64, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pbFilter := filterToPbFilter(filter)
	r, err := cli.GetMaxSeq(ctx, &pb.GetMaxSeqRequest{
		Filter: pbFilter,
	})
	if err != nil {
		return 0, faults.Errorf("could not get last event id: %w", err)
	}

	return r.Sequence, nil
}

func (c GrpcRepository) GetEvents(ctx context.Context, afterSeq uint64, limit int, filter store.Filter) ([]*eventsourcing.Event, error) {
	cli, conn, err := c.dial()
	if err != nil {
		return nil, faults.Wrap(err)
	}
	defer conn.Close()

	pbFilter := filterToPbFilter(filter)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := cli.GetEvents(ctx, &pb.GetEventsRequest{
		AfterSequence: afterSeq,
		Limit:         int32(limit),
		Filter:        pbFilter,
	})
	if err != nil {
		return nil, faults.Errorf("could not get events: %w", err)
	}

	events := make([]*eventsourcing.Event, len(r.Events))
	for k, v := range r.Events {
		createdAt, err := tsToTime(v.CreatedAt)
		if err != nil {
			return nil, faults.Errorf("could convert timestamp to time: %w", err)
		}
		eID, err := eventid.Parse(v.Id)
		if err != nil {
			return nil, faults.Errorf("unable to parse message ID '%s': %w", v.Id, err)
		}
		events[k] = &eventsourcing.Event{
			ID:               eID,
			AggregateID:      v.AggregateId,
			AggregateIDHash:  v.AggregateIdHash,
			AggregateVersion: v.AggregateVersion,
			AggregateKind:    eventsourcing.Kind(v.AggregateKind),
			Kind:             eventsourcing.Kind(v.Kind),
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Metadata:         encoding.JSONOfString(v.Metadata),
			CreatedAt:        *createdAt,
			Sequence:         v.Sequence,
		}
	}
	return events, nil
}

func filterToPbFilter(filter store.Filter) *pb.Filter {
	types := make([]string, len(filter.AggregateKinds))
	for k, v := range filter.AggregateKinds {
		types[k] = v.String()
	}
	metadata := []*pb.Metadata{}
	for key, v := range filter.Metadata {
		for _, value := range v {
			metadata = append(metadata, &pb.Metadata{Key: key, Value: value})
		}
	}
	return &pb.Filter{
		AggregateKinds: types,
		Metadata:       metadata,
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
