package clusterapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"

	clusterv1 "github.com/LicoLand/BadTower/gen/go/badtower/cluster/v1"
	"github.com/LicoLand/BadTower/internal/stationcluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxClusterMessageBytes = 1 << 20

// Server exposes only the privacy-minimal cluster operations over a local
// listener. Its bearer credential is independent from native Station and peer
// mTLS credentials.
type Server struct {
	clusterv1.UnimplementedClusterOperatorServer
	operator *stationcluster.Operator
	digest   string
}

func NewServer(operator *stationcluster.Operator, credentialSHA256 string) (*Server, error) {
	raw, decodeErr := base64.RawURLEncoding.DecodeString(credentialSHA256)
	if operator == nil || decodeErr != nil || len(raw) != sha256.Size {
		return nil, errors.New("invalid cluster operator configuration")
	}
	return &Server{operator: operator, digest: credentialSHA256}, nil
}

func (server *Server) GRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxClusterMessageBytes), grpc.MaxSendMsgSize(maxClusterMessageBytes), grpc.MaxConcurrentStreams(16), grpc.UnaryInterceptor(server.authorize))
	clusterv1.RegisterClusterOperatorServer(grpcServer, server)
	return grpcServer
}

func (server *Server) authorize(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "cluster operator authentication failed")
	}
	token := strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer "))
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "cluster operator authentication failed")
	}
	sum := sha256.Sum256([]byte(token))
	digest := base64.RawURLEncoding.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(digest), []byte(server.digest)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "cluster operator authentication failed")
	}
	return handler(ctx, request)
}

func (server *Server) Status(ctx context.Context, _ *clusterv1.StatusRequest) (*clusterv1.StatusResponse, error) {
	value, err := server.operator.Status(ctx)
	if err != nil {
		return nil, apiError(err)
	}
	response := &clusterv1.StatusResponse{MemberId: value.MemberID, LeaderMemberId: value.LeaderMemberID, AppliedIndex: value.AppliedIndex, PlacementEpoch: value.PlacementEpoch, MemberCount: value.MemberCount}
	if value.ActiveMove != nil {
		response.MovePhase = string(value.ActiveMove.Phase)
	}
	return response, nil
}

func mutation(state stationcluster.ControlState, err error) (*clusterv1.MutationResponse, error) {
	if err != nil {
		return nil, apiError(err)
	}
	return &clusterv1.MutationResponse{PlacementEpoch: state.PlacementEpoch, MemberCount: uint32(len(state.Members))}, nil
}

func member(value *clusterv1.Member) (stationcluster.ControlMember, error) {
	if value == nil {
		return stationcluster.ControlMember{}, status.Error(codes.InvalidArgument, "member is required")
	}
	return stationcluster.ControlMember{ID: value.Id, FaultDomain: value.FaultDomain, Incarnation: value.Incarnation, CommandVersion: value.CommandVersion, StoreVersion: value.StoreVersion}, nil
}

func (server *Server) Bootstrap(ctx context.Context, _ *clusterv1.BootstrapRequest) (*clusterv1.MutationResponse, error) {
	return mutation(server.operator.Bootstrap(ctx))
}
func (server *Server) Join(ctx context.Context, request *clusterv1.JoinRequest) (*clusterv1.MutationResponse, error) {
	value, err := member(request.Member)
	if err != nil {
		return nil, err
	}
	return mutation(server.operator.Join(ctx, value))
}
func (server *Server) CaughtUp(ctx context.Context, request *clusterv1.CaughtUpRequest) (*clusterv1.MutationResponse, error) {
	return mutation(server.operator.MarkCaughtUp(ctx, request.MemberId, request.AppliedIndex))
}
func (server *Server) Promote(ctx context.Context, request *clusterv1.MemberRequest) (*clusterv1.MutationResponse, error) {
	return mutation(server.operator.Promote(ctx, request.MemberId))
}
func (server *Server) Drain(ctx context.Context, request *clusterv1.MemberRequest) (*clusterv1.MutationResponse, error) {
	return mutation(server.operator.Drain(ctx, request.MemberId))
}
func (server *Server) Remove(ctx context.Context, request *clusterv1.MemberRequest) (*clusterv1.MutationResponse, error) {
	return mutation(server.operator.Remove(ctx, request.MemberId))
}
func (server *Server) Replace(ctx context.Context, request *clusterv1.ReplaceRequest) (*clusterv1.MutationResponse, error) {
	value, err := member(request.Replacement)
	if err != nil {
		return nil, err
	}
	return mutation(server.operator.Replace(ctx, request.OldMemberId, value))
}

func (server *Server) Rebalance(ctx context.Context, request *clusterv1.RebalanceRequest) (*clusterv1.RebalanceResponse, error) {
	move, err := server.operator.Rebalance(ctx, request.DrainingMemberId, request.ReplacementMemberId)
	if err != nil {
		return nil, apiError(err)
	}
	if move == nil {
		return &clusterv1.RebalanceResponse{Complete: true}, nil
	}
	return &clusterv1.RebalanceResponse{Partition: uint32(move.Partition), FromMemberId: move.FromMemberID, ToMemberId: move.ToMemberID, ExpectedEpoch: move.ExpectedEpoch, Phase: string(move.Phase), Complete: move.Phase == stationcluster.MovePhaseCommitted}, nil
}

func (server *Server) Backup(ctx context.Context, request *clusterv1.BackupRequest) (*clusterv1.BackupResponse, error) {
	manifest, err := server.operator.Backup(ctx, request.ArtifactRoot, int(request.ChunkBytes))
	if err != nil {
		return nil, apiError(err)
	}
	return &clusterv1.BackupResponse{PlacementEpoch: manifest.PlacementEpoch, ControlAppliedIndex: manifest.ControlApplied, DigestSha256: manifest.DigestSHA256}, nil
}

func (server *Server) RestorePreflight(_ context.Context, request *clusterv1.RestorePreflightRequest) (*clusterv1.RestorePreflightResponse, error) {
	manifest, err := server.operator.RestorePreflight(request.BackupRoot, request.IsolatedRoot, stationcluster.RestorePreflight{ClusterID: request.ClusterId, PlacementEpoch: request.PlacementEpoch, MinCommandVersion: request.MinCommandVersion, MinStoreVersion: request.MinStoreVersion, MaxCommandVersion: request.MaxCommandVersion, MaxStoreVersion: request.MaxStoreVersion})
	if err != nil {
		return nil, apiError(err)
	}
	return &clusterv1.RestorePreflightResponse{PlacementEpoch: manifest.PlacementEpoch, ControlAppliedIndex: manifest.ControlApplied, DigestSha256: manifest.DigestSHA256}, nil
}

func (server *Server) UpgradePreflight(ctx context.Context, request *clusterv1.UpgradePreflightRequest) (*clusterv1.UpgradePreflightResponse, error) {
	if request.Current == nil || request.Next == nil {
		return nil, status.Error(codes.InvalidArgument, "versions are required")
	}
	steps, err := server.operator.UpgradePreflight(ctx, stationcluster.UpgradePreflight{Current: stationcluster.ClusterVersion{Command: request.Current.Command, Store: request.Current.Store}, Next: stationcluster.ClusterVersion{Command: request.Next.Command, Store: request.Next.Store}, LeaderMemberIDs: request.LeaderMemberIds, UnavailableMemberIDs: request.UnavailableMemberIds})
	if err != nil {
		return nil, apiError(err)
	}
	response := &clusterv1.UpgradePreflightResponse{Steps: make([]*clusterv1.UpgradeStep, 0, len(steps))}
	for _, step := range steps {
		response.Steps = append(response.Steps, &clusterv1.UpgradeStep{Sequence: uint32(step.Sequence), MemberId: step.MemberID, Stage: string(step.Stage), From: &clusterv1.Version{Command: step.From.Command, Store: step.From.Store}, To: &clusterv1.Version{Command: step.To.Command, Store: step.To.Store}})
	}
	return response, nil
}

func apiError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "cluster operation deadline exceeded")
	case errors.Is(err, stationcluster.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, "cluster operation requires leader")
	case errors.Is(err, stationcluster.ErrStaleEpoch), errors.Is(err, stationcluster.ErrControlConflict):
		return status.Error(codes.Aborted, "cluster state changed")
	case errors.Is(err, stationcluster.ErrInvalidTopology), errors.Is(err, stationcluster.ErrControlOrder):
		return status.Error(codes.FailedPrecondition, "cluster operation rejected")
	default:
		return status.Error(codes.Unavailable, "cluster operation unavailable")
	}
}
