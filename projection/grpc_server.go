package projection

import (
	"context"
	"encoding/json"
	"net"

	"github.com/golang/protobuf/ptypes"
	"github.com/quintans/faults"
	"google.golang.org/grpc"

	"github.com/quintans/eventsourcing"

	pb "github.com/quintans/eventsourcing/api/proto"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

type GrpcServer struct {
	store EventsRepository
}

func (s *GrpcServer) GetEvents(ctx context.Context, r *pb.GetEventsRequest) (*pb.GetEventsReply, error) {
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
		createdAt, err := ptypes.TimestampProto(v.CreatedAt)
		if err != nil {
			return nil, faults.Errorf("could convert timestamp to proto: %w", err)
		}
		metadata, err := json.Marshal(v.Metadata)
		if err != nil {
			return nil, faults.Errorf("unable marshal metadata: %w", err)
		}
		pbEvents[k] = &pb.Event{
			Id:               v.ID.String(),
			AggregateId:      v.AggregateID,
			AggregateVersion: v.AggregateVersion,
			AggregateKind:    v.AggregateKind.String(),
			Kind:             v.Kind.String(),
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Metadata:         string(metadata),
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
	metadata := store.Metadata{}
	for _, v := range pbFilter.Metadata {
		values := metadata[v.Key]
		if values == nil {
			values = []string{v.Value}
		} else {
			values = append(values, v.Value)
		}
		metadata[v.Key] = values
	}
	return store.Filter{
		AggregateKinds: types,
		Metadata:       metadata,
		Splits:         pbFilter.Splits,
		Split:          pbFilter.Split,
	}
}

func StartGrpcServer(ctx context.Context, address string, repo EventsRepository) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return faults.Errorf("failed to listen: %w", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &GrpcServer{store: repo})

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		return faults.Errorf("failed to serve: %w", err)
	}
	return nil
}
