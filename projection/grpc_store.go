package projection

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"
	"google.golang.org/grpc"

	"github.com/quintans/eventsourcing"
	pb "github.com/quintans/eventsourcing/api/proto"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

type GrpcRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	address string
}

var _ EventsRepository[ulid.ULID] = (*GrpcRepository[ulid.ULID, *ulid.ULID])(nil)

func NewGrpcRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]](address string) GrpcRepository[K, PK] {
	return GrpcRepository[K, PK]{
		address: address,
	}
}

func (c GrpcRepository[K, PK]) GetEvents(ctx context.Context, after, until eventid.EventID, limit int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	cli, conn, err := c.dial()
	if err != nil {
		return nil, faults.Wrap(err)
	}
	defer conn.Close()

	pbFilter := filterToPbFilter(filter)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := cli.GetEvents(ctx, &pb.GetEventsRequest{
		AfterEventId: after.String(),
		UntilEventId: until.String(),
		Limit:        int32(limit),
		Filter:       pbFilter,
	})
	if err != nil {
		return nil, faults.Errorf("could not get events: %w", err)
	}

	events := make([]*eventsourcing.Event[K], len(r.Events))
	for k, v := range r.Events {
		createdAt, err := tsToTime(v.CreatedAt)
		if err != nil {
			return nil, faults.Errorf("could convert timestamp to time: %w", err)
		}
		eID, err := eventid.Parse(v.Id)
		if err != nil {
			return nil, faults.Errorf("unable to parse message ID '%s': %w", v.Id, err)
		}

		idPtr := PK(new(K))
		err = idPtr.UnmarshalText([]byte(v.AggregateId))
		if err != nil {
			return nil, faults.Errorf("unmarshaling id '%s': %w", v.AggregateId, err)
		}

		events[k] = &eventsourcing.Event[K]{
			ID:               eID,
			AggregateID:      *idPtr,
			AggregateVersion: v.AggregateVersion,
			AggregateKind:    eventsourcing.Kind(v.AggregateKind),
			Kind:             eventsourcing.Kind(v.Kind),
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Metadata:         v.Metadata,
			CreatedAt:        *createdAt,
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
	for _, v := range filter.Metadata {
		metadata = append(metadata, &pb.Metadata{Key: v.Key, Value: v.Values})
	}
	return &pb.Filter{
		AggregateKinds: types,
		Metadata:       metadata,
		Splits:         filter.Splits,
		Split:          filter.Split,
	}
}

func (c GrpcRepository[K, PK]) dial() (pb.StoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, faults.Errorf("did not connect: %w", err)
	}
	return pb.NewStoreClient(conn), conn, nil
}

func tsToTime(ts *timestamp.Timestamp) (*time.Time, error) {
	var exp *time.Time
	if ts != nil {
		t := ts.AsTime()
		exp = &t
	}
	return exp, nil
}
