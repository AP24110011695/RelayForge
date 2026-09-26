package broker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	relayforgev1 "relayforge/api/proto/relayforge/v1"

	"relayforge/internal/logengine"
	"relayforge/internal/metadata"
	"relayforge/internal/record"
)

// gRPC message-size limits (bounded memory on both directions).
const (
	MaxRecvMsgSize = 16 << 20
	MaxSendMsgSize = 16 << 20
)

// NewServer wires the broker into a gRPC server: the RelayForge service,
// the health protocol, message-size limits, and the recovery + logging
// interceptors. Serving status must be set by the caller once ready
// (health.NewServer() is returned registered on the server).
func NewServer(b *Broker, log *slog.Logger) (*grpc.Server, *health.Server) {
	if log == nil {
		log = slog.Default()
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxRecvMsgSize),
		grpc.MaxSendMsgSize(MaxSendMsgSize),
		grpc.ChainUnaryInterceptor(recoveryInterceptor(log), loggingInterceptor(log)),
	)
	relayforgev1.RegisterRelayForgeServer(srv, &server{b: b})
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	return srv, hs
}

type server struct {
	relayforgev1.UnimplementedRelayForgeServer
	b *Broker
}

func (s *server) Produce(ctx context.Context, req *relayforgev1.ProduceRequest) (*relayforgev1.ProduceResponse, error) {
	msgs := make([]Message, len(req.GetMessages()))
	for i, m := range req.GetMessages() {
		msgs[i] = Message{Key: m.GetKey(), Value: m.GetValue()}
	}
	offsets, err := s.b.Produce(ctx, req.GetTopic(), req.GetPartition(), mapAck(req.GetAckMode()), msgs)
	if err != nil {
		return nil, toStatus(err)
	}
	return &relayforgev1.ProduceResponse{Offsets: offsets}, nil
}

func (s *server) Fetch(ctx context.Context, req *relayforgev1.FetchRequest) (*relayforgev1.FetchResponse, error) {
	recs, next, hw, err := s.b.Fetch(ctx, req.GetTopic(), req.GetPartition(), req.GetOffset(),
		int(req.GetMaxBytes()), time.Duration(req.GetMaxWaitMs())*time.Millisecond)
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*relayforgev1.FetchedRecord, len(recs))
	for i, r := range recs {
		out[i] = &relayforgev1.FetchedRecord{
			Offset:      r.Offset,
			Key:         r.Key,
			Value:       r.Value,
			TimestampMs: r.Timestamp,
		}
	}
	return &relayforgev1.FetchResponse{Records: out, NextOffset: next, HighWatermark: hw}, nil
}

func mapAck(m relayforgev1.AckMode) Ack {
	switch m {
	case relayforgev1.AckMode_ACK_MODE_NONE:
		return AckNone
	case relayforgev1.AckMode_ACK_MODE_FLUSHED:
		return AckFlushed
	case relayforgev1.AckMode_ACK_MODE_WRITTEN, relayforgev1.AckMode_ACK_MODE_UNSPECIFIED:
		return AckWritten
	default:
		return AckWritten
	}
}

// toStatus maps broker/engine/metadata errors to gRPC status codes per the
// table in docs/PROTOCOL.md. Unknown errors map to Internal.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnknownTopicOrPartition):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, metadata.ErrTopicNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, logengine.ErrOffsetOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, ErrBatchTooLarge), errors.Is(err, record.ErrRecordTooLarge), errors.Is(err, ErrOverloaded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrEmptyBatch), errors.Is(err, metadata.ErrInvalidTopic):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, metadata.ErrTopicExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrPartitionFailed), errors.Is(err, ErrClosed), errors.Is(err, logengine.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *server) CreateTopic(ctx context.Context, req *relayforgev1.CreateTopicRequest) (*relayforgev1.CreateTopicResponse, error) {
	if err := s.b.CreateTopic(req.GetName(), req.GetPartitionCount()); err != nil {
		return nil, toStatus(err)
	}
	return &relayforgev1.CreateTopicResponse{}, nil
}

func (s *server) ListTopics(ctx context.Context, req *relayforgev1.ListTopicsRequest) (*relayforgev1.ListTopicsResponse, error) {
	topics := s.b.ListTopics()
	out := make([]*relayforgev1.TopicInfo, len(topics))
	for i, t := range topics {
		out[i] = &relayforgev1.TopicInfo{Name: t.Name, PartitionCount: t.PartitionCount}
	}
	return &relayforgev1.ListTopicsResponse{Topics: out}, nil
}

func (s *server) DescribeTopic(ctx context.Context, req *relayforgev1.DescribeTopicRequest) (*relayforgev1.DescribeTopicResponse, error) {
	t, err := s.b.DescribeTopic(req.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	return &relayforgev1.DescribeTopicResponse{Topic: &relayforgev1.TopicInfo{Name: t.Name, PartitionCount: t.PartitionCount}}, nil
}

func recoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in rpc", "method", info.FullMethod, "panic", r)
				err = status.Error(codes.Internal, "internal panic")
			}
		}()
		return handler(ctx, req)
	}
}

func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		log.Debug("rpc", "method", info.FullMethod, "duration", time.Since(start), "error", err)
		return resp, err
	}
}
