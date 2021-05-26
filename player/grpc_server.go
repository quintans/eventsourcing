package player

import (
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/lib/pq"
	"github.com/quintans/faults"
	"google.golang.org/grpc"

	"github.com/quintans/eventsourcing"

	pb "github.com/quintans/eventsourcing/api/proto"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

type GrpcServer struct {
	store Repository
}

func (s *GrpcServer) GetLastEventID(ctx context.Context, r *pb.GetLastEventIDRequest) (*pb.GetLastEventIDReply, error) {
	filter := pbFilterToFilter(r.GetFilter())
	eID, err := s.store.GetLastEventID(ctx, time.Duration(r.TrailingLag)*time.Millisecond, filter)
	if err != nil {
		return nil, err
	}
	return &pb.GetLastEventIDReply{EventId: eID.String()}, nil
}

func (s *GrpcServer) GetEvents(ctx context.Context, r *pb.GetEventsRequest) (*pb.GetEventsReply, error) {
	filter := pbFilterToFilter(r.GetFilter())
	afterEventID, err := eventid.Parse(r.GetAfterEventId())
	if err != nil {
		return nil, faults.Errorf("unable to parse afterEventID '%s': %w", r.GetAfterEventId(), err)
	}
	events, err := s.store.GetEvents(ctx, afterEventID, int(r.GetLimit()), time.Duration(r.TrailingLag)*time.Millisecond, filter)
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
			AggregateId:      v.AggregateID.String(),
			AggregateIdHash:  v.AggregateIDHash,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType.String(),
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
	types := make([]eventsourcing.AggregateType, len(pbFilter.AggregateTypes))
	for k, v := range pbFilter.AggregateTypes {
		types[k] = eventsourcing.AggregateType(v)
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
		AggregateTypes: types,
		Metadata:       metadata,
		Partitions:     pbFilter.Partitions,
		PartitionLow:   pbFilter.PartitionLow,
		PartitionHi:    pbFilter.PartitionHi,
	}
}

func StartGrpcServer(ctx context.Context, address string, repo Repository) error {
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
