package clusterapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"testing"

	clusterv1 "github.com/LicoLand/BadTower/gen/go/badtower/cluster/v1"
	"github.com/LicoLand/BadTower/internal/stationcluster"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type apiControl struct {
	machine *stationcluster.ControlMachine
}

func (control apiControl) ID() uint64                                  { return 1 }
func (control apiControl) Status() (uint64, uint64, error)             { return 1, 7, nil }
func (control apiControl) Campaign(context.Context) error              { return nil }
func (control apiControl) Step(context.Context, *raftpb.Message) error { return nil }
func (control apiControl) Propose(_ context.Context, command stationcluster.ControlCommand) (stationcluster.ControlState, error) {
	return control.machine.Apply(command)
}
func (control apiControl) ProposeMembership(context.Context, stationcluster.ControlMembershipAction, uint64) error {
	return nil
}
func (control apiControl) ReadBarrier(context.Context) error { return nil }
func (control apiControl) State(context.Context) (stationcluster.ControlState, error) {
	return control.machine.Snapshot(), nil
}
func (control apiControl) Close() error { return nil }

type apiMembership struct{}

func (apiMembership) InstallLearnerSnapshot(context.Context, uint16, uint64, uint64) error {
	return nil
}
func (apiMembership) WaitLearnerCaughtUp(context.Context, uint16, uint64, uint64) (uint64, error) {
	return 1, nil
}
func (apiMembership) EnterJoint(context.Context, uint16, uint64, uint64, uint64) error { return nil }
func (apiMembership) MoveLeadershipOff(context.Context, uint16, uint64, uint64, uint64) error {
	return nil
}
func (apiMembership) LeaveJoint(context.Context, uint16, uint64) error            { return nil }
func (apiMembership) RemoveReplica(context.Context, uint16, uint64, uint64) error { return nil }

func TestClusterOperatorCredentialIsIndependentAndRequired(t *testing.T) {
	machine, err := stationcluster.OpenControlMachine(stationcluster.ControlBootstrap{Root: t.TempDir(), ClusterID: "cluster-api-auth", PlacementEpoch: 1, MinCommandVersion: 1, MinStoreVersion: stationcluster.StoreVersion, Members: []stationcluster.ControlMember{{ID: 1, FaultDomain: "a", Incarnation: 1, CommandVersion: 1, StoreVersion: stationcluster.StoreVersion}, {ID: 2, FaultDomain: "b", Incarnation: 1, CommandVersion: 1, StoreVersion: stationcluster.StoreVersion}, {ID: 3, FaultDomain: "c", Incarnation: 1, CommandVersion: 1, StoreVersion: stationcluster.StoreVersion}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = machine.Close() })
	operator, err := stationcluster.NewOperatorWithControl(apiControl{machine}, apiMembership{}, 1, stationcluster.StoreVersion)
	if err != nil {
		t.Fatal(err)
	}
	mintBearer := func() string {
		var raw [24]byte
		if _, err := rand.Read(raw[:]); err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw[:])
	}
	token := mintBearer()
	sum := sha256.Sum256([]byte(token))
	server, err := NewServer(operator, base64.RawURLEncoding.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := server.GRPCServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///cluster", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := clusterv1.NewClusterOperatorClient(conn)
	if _, err = client.Status(context.Background(), &clusterv1.StatusRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing credential=%v", err)
	}
	wrong := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+mintBearer()))
	if _, err = client.Status(wrong, &clusterv1.StatusRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("native credential admitted=%v", err)
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	value, err := client.Status(ctx, &clusterv1.StatusRequest{})
	if err != nil || value.LeaderMemberId != 1 || value.MemberCount != 3 {
		t.Fatalf("status=%+v err=%v", value, err)
	}
}
