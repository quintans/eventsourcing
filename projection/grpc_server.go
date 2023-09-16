package projection

import (
	"context"
	"net"

	"github.com/quintans/eventsourcing"
	pb "github.com/quintans/eventsourcing/api/proto"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcServer[K eventsourcing.ID] struct {
	store EventsRepository[K]
}

func (s *GrpcServer[K]) GetEvents(ctx context.Context, r *pb.GetEventsRequest) (*pb.GetEventsReply, error) {
	filter := pbFilterToFilter(r.GetFilter())
	after, err := eventid.Parse(r.GetAfterEventId())
	if err != nil {
		return nil, faults.Wrap(err)
	}
	until, err := eventid.Parse(r.GetUntilEventId())
	if err != nil {
		return nil, faults.Wrap(err)
	}

	events, err := s.store.GetEvents(ctx, after, until, int(r.GetLimit()), filter)
	if err != nil {
		return nil, err
	}
	pbEvents := make([]*pb.Event, len(events))
	for k, v := range events {
		createdAt := timestamppb.New(v.CreatedAt)
		pbEvents[k] = &pb.Event{
			Id:               v.ID.String(),
			AggregateId:      v.AggregateID.String(),
			AggregateVersion: v.AggregateVersion,
			AggregateKind:    v.AggregateKind.String(),
			Kind:             v.Kind.String(),
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Metadata:         v.Metadata,
			CreatedAt:        createdAt,
		}
	}
	return &pb.GetEventsReply{Events: pbEvents}, nil
}

func pbFilterToFilter(pbFilter *pb.Filter) store.Filter {
	types := make([]eventsourcing.Kind, len(pbFilter.AggregateKinds))
	for k, v := range pbFilter.AggregateKinds {
		types[k] = eventsourcing.Kind(v)
	}
	metadata := store.MetadataFilter{}
	for _, kv := range pbFilter.Metadata {
		metadata = append(metadata, &store.MetadataKVs{
			Key:    kv.Key,
			Values: kv.Value,
		})
	}
	return store.Filter{
		AggregateKinds: types,
		Metadata:       metadata,
		Splits:         pbFilter.Splits,
		SplitIDs:       pbFilter.SplitIds,
	}
}

func StartGrpcServer[K eventsourcing.ID](ctx context.Context, address string, repo EventsRepository[K]) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return faults.Errorf("failed to listen: %w", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &GrpcServer[K]{store: repo})

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		return faults.Errorf("failed to serve: %w", err)
	}
	return nil
}
