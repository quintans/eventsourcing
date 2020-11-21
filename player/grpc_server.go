package player

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/lib/pq"
	pb "github.com/quintans/eventstore/api/proto"
	"github.com/quintans/eventstore/repo"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	repo Repository
}

func (s *GrpcServer) GetLastEventID(ctx context.Context, r *pb.GetLastEventIDRequest) (*pb.GetLastEventIDReply, error) {
	filter := pbFilterToFilter(r.GetFilter())
	eID, err := s.repo.GetLastEventID(ctx, time.Duration(r.TrailingLag)*time.Millisecond, filter)
	if err != nil {
		return nil, err
	}
	return &pb.GetLastEventIDReply{EventId: eID}, nil
}

func (s *GrpcServer) GetEvents(ctx context.Context, r *pb.GetEventsRequest) (*pb.GetEventsReply, error) {
	filter := pbFilterToFilter(r.GetFilter())
	events, err := s.repo.GetEvents(ctx, r.GetAfterEventId(), int(r.GetLimit()), time.Duration(r.TrailingLag)*time.Millisecond, filter)
	if err != nil {
		return nil, err
	}
	pbEvents := make([]*pb.Event, len(events))
	for k, v := range events {
		createdAt, err := ptypes.TimestampProto(v.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("could convert timestamp to proto: %w", err)
		}
		labels, err := json.Marshal(v.Labels)
		if err != nil {
			return nil, fmt.Errorf("Unable marshal labels: %w", err)
		}
		pbEvents[k] = &pb.Event{
			Id:               v.ID,
			AggregateId:      v.AggregateID,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           string(labels),
			CreatedAt:        createdAt,
		}
	}
	return &pb.GetEventsReply{Events: pbEvents}, nil
}

func pbFilterToFilter(pbFilter *pb.Filter) repo.Filter {
	types := make([]string, len(pbFilter.AggregateTypes))
	for k, v := range pbFilter.AggregateTypes {
		types[k] = v
	}
	labels := make([]repo.Label, len(pbFilter.Labels))
	for k, v := range pbFilter.Labels {
		labels[k] = repo.NewLabel(v.Key, v.Value)
	}
	return repo.Filter{AggregateTypes: types, Labels: labels}
}

func StartGrpcServer(ctx context.Context, address string, repo Repository) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &GrpcServer{repo: repo})

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}
