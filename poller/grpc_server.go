package poller

import (
	"context"
	"fmt"
	"net"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/lib/pq"
	pb "github.com/quintans/eventstore/api/proto"
	"github.com/quintans/eventstore/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	repo Repository
}

func (s *GrpcServer) GetLastEventID(ctx context.Context, _ *pb.GetLastEventIDRequest) (*pb.GetLastEventIDReply, error) {
	eID, err := s.repo.GetLastEventID(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetLastEventIDReply{EventId: eID}, nil
}

func (s *GrpcServer) GetEvents(ctx context.Context, r *pb.GetEventsRequest) (*pb.GetEventsReply, error) {
	pbFilter := r.GetFilter()
	types := make([]string, len(pbFilter.AggregateTypes))
	for k, v := range pbFilter.AggregateTypes {
		types[k] = v
	}
	labels := make([]common.Label, len(pbFilter.Labels))
	for k, v := range pbFilter.Labels {
		labels[k] = common.NewLabel(v.Key, v.Value)
	}

	events, err := s.repo.GetEvents(ctx, r.GetAfterEventId(), int(r.GetLimit()), common.Filter{AggregateTypes: types, Labels: labels})
	if err != nil {
		return nil, err
	}
	pbEvents := make([]*pb.Event, len(events))
	for k, v := range events {
		createdAt, err := ptypes.TimestampProto(v.CreatedAt)
		if err != nil {
			log.Fatal("could convert time")
		}

		pbEvents[k] = &pb.Event{
			Id:               v.ID,
			AggregateId:      v.AggregateID,
			AggregateVersion: int32(v.AggregateVersion),
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             string(v.Body),
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           string(v.Labels),
			CreatedAt:        createdAt,
		}
	}
	return &pb.GetEventsReply{Events: pbEvents}, nil
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
