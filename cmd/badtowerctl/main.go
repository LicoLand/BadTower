package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	clusterv1 "github.com/LicoLand/BadTower/gen/go/badtower/cluster/v1"
	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"github.com/LicoLand/BadTower/internal/nativeapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("usage: badtowerctl <check-config|credential|reserve|submit|claim|settle|capacity> ...")
	}
	switch arguments[0] {
	case "check-config":
		return runCheckConfig(arguments[1:])
	case "credential":
		return runCredential(arguments[1:])
	case "reserve", "submit", "claim", "settle", "capacity":
		return runDataCommand(arguments[0], arguments[1:])
	case "cluster":
		return runClusterCommand(arguments[1:])
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}

func runClusterCommand(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("usage: badtowerctl cluster <status|bootstrap|join|caught-up|promote|drain|replace|remove|rebalance|backup|restore-preflight|upgrade-preflight> ...")
	}
	operation := arguments[0]
	flagSet := flag.NewFlagSet("cluster "+operation, flag.ContinueOnError)
	socket := flagSet.String("socket", "", "cluster operator unix socket")
	credentialFile := flagSet.String("credential-file", "", "mode-0600 cluster operator credential")
	memberID := flagSet.Uint64("member-id", 0, "member id")
	oldMemberID := flagSet.Uint64("old-member-id", 0, "old member id")
	replacementID := flagSet.Uint64("replacement-member-id", 0, "replacement member id")
	faultDomain := flagSet.String("fault-domain", "", "fault domain")
	incarnation := flagSet.Uint64("incarnation", 0, "member incarnation")
	commandVersion := flagSet.Uint("command-version", 0, "command version")
	storeVersion := flagSet.Uint("store-version", 0, "store version")
	appliedIndex := flagSet.Uint64("applied-index", 0, "applied index")
	artifactRoot := flagSet.String("artifact-root", "", "backup artifact root")
	backupRoot := flagSet.String("backup-root", "", "backup root")
	isolatedRoot := flagSet.String("isolated-root", "", "fresh isolated restore root")
	clusterID := flagSet.String("cluster-id", "", "cluster id")
	placementEpoch := flagSet.Uint64("placement-epoch", 0, "placement epoch")
	chunkBytes := flagSet.Uint("chunk-bytes", 0, "backup snapshot chunk bytes")
	nextCommand := flagSet.Uint("next-command-version", 0, "next command version")
	nextStore := flagSet.Uint("next-store-version", 0, "next store version")
	var leaders, unavailable uint64List
	flagSet.Var(&leaders, "leader-member-id", "leader member id, repeatable")
	flagSet.Var(&unavailable, "unavailable-member-id", "unavailable member id, repeatable")
	if err := flagSet.Parse(arguments[1:]); err != nil {
		return err
	}
	if *socket == "" || *credentialFile == "" {
		return errors.New("--socket and --credential-file are required")
	}
	token, err := readCredential(*credentialFile)
	if err != nil {
		return err
	}
	socketPath, err := nativeapi.VerifyUnixSocketPath(*socket)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(
		"passthrough:///unix-cluster",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return nativeapi.DialVerifiedUnix(ctx, socketPath)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := clusterv1.NewClusterOperatorClient(conn)
	ctx, cancel := context.WithTimeout(withAuth(context.Background(), token), 30*time.Second)
	defer cancel()
	member := func(id uint64) *clusterv1.Member {
		return &clusterv1.Member{Id: id, FaultDomain: *faultDomain, Incarnation: *incarnation, CommandVersion: uint32(*commandVersion), StoreVersion: uint32(*storeVersion)}
	}
	switch operation {
	case "status":
		value, err := client.Status(ctx, &clusterv1.StatusRequest{})
		if err != nil {
			return err
		}
		fmt.Printf("member_id=%d leader_member_id=%d applied_index=%d placement_epoch=%d member_count=%d move_phase=%s\n", value.MemberId, value.LeaderMemberId, value.AppliedIndex, value.PlacementEpoch, value.MemberCount, value.MovePhase)
	case "bootstrap":
		_, err = client.Bootstrap(ctx, &clusterv1.BootstrapRequest{})
	case "join":
		_, err = client.Join(ctx, &clusterv1.JoinRequest{Member: member(*memberID)})
	case "caught-up":
		_, err = client.CaughtUp(ctx, &clusterv1.CaughtUpRequest{MemberId: *memberID, AppliedIndex: *appliedIndex})
	case "promote":
		_, err = client.Promote(ctx, &clusterv1.MemberRequest{MemberId: *memberID})
	case "drain":
		_, err = client.Drain(ctx, &clusterv1.MemberRequest{MemberId: *memberID})
	case "replace":
		_, err = client.Replace(ctx, &clusterv1.ReplaceRequest{OldMemberId: *oldMemberID, Replacement: member(*replacementID)})
	case "remove":
		_, err = client.Remove(ctx, &clusterv1.MemberRequest{MemberId: *memberID})
	case "rebalance":
		var value *clusterv1.RebalanceResponse
		value, err = client.Rebalance(ctx, &clusterv1.RebalanceRequest{DrainingMemberId: *memberID, ReplacementMemberId: *replacementID})
		if err == nil {
			fmt.Printf("partition=%d phase=%s complete=%t\n", value.Partition, value.Phase, value.Complete)
		}
	case "backup":
		var value *clusterv1.BackupResponse
		value, err = client.Backup(ctx, &clusterv1.BackupRequest{ArtifactRoot: *artifactRoot, ChunkBytes: uint32(*chunkBytes)})
		if err == nil {
			fmt.Printf("placement_epoch=%d control_applied_index=%d digest_sha256=%s\n", value.PlacementEpoch, value.ControlAppliedIndex, value.DigestSha256)
		}
	case "restore-preflight":
		var value *clusterv1.RestorePreflightResponse
		value, err = client.RestorePreflight(ctx, &clusterv1.RestorePreflightRequest{BackupRoot: *backupRoot, IsolatedRoot: *isolatedRoot, ClusterId: *clusterID, PlacementEpoch: *placementEpoch, MinCommandVersion: uint32(*commandVersion), MinStoreVersion: uint32(*storeVersion), MaxCommandVersion: uint32(*nextCommand), MaxStoreVersion: uint32(*nextStore)})
		if err == nil {
			fmt.Printf("placement_epoch=%d control_applied_index=%d digest_sha256=%s\n", value.PlacementEpoch, value.ControlAppliedIndex, value.DigestSha256)
		}
	case "upgrade-preflight":
		var value *clusterv1.UpgradePreflightResponse
		value, err = client.UpgradePreflight(ctx, &clusterv1.UpgradePreflightRequest{Current: &clusterv1.Version{Command: uint32(*commandVersion), Store: uint32(*storeVersion)}, Next: &clusterv1.Version{Command: uint32(*nextCommand), Store: uint32(*nextStore)}, LeaderMemberIds: leaders, UnavailableMemberIds: unavailable})
		if err == nil {
			fmt.Printf("steps=%d\n", len(value.Steps))
		}
	default:
		return fmt.Errorf("unknown cluster command %q", operation)
	}
	return err
}

func runCheckConfig(arguments []string) error {
	flagSet := flag.NewFlagSet("check-config", flag.ContinueOnError)
	configPath := flagSet.String("config", "", "configuration path")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	_, err := nativeapi.LoadConfig(*configPath)
	return err
}

func runCredential(arguments []string) error {
	if len(arguments) >= 1 && arguments[0] == "generate" {
		flagSet := flag.NewFlagSet("credential generate", flag.ContinueOnError)
		outputPath := flagSet.String("output", "", "mode-0600 credential output path")
		if err := flagSet.Parse(arguments[1:]); err != nil {
			return err
		}
		if *outputPath == "" {
			return errors.New("--output is required")
		}
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return err
		}
		token, _, err := nativeapi.GenerateToken(raw[:])
		if err != nil {
			return err
		}
		file, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if _, err := file.Write([]byte(token + "\n")); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		return file.Close()
	}
	return errors.New("usage: badtowerctl credential generate --output <file>")
}

func runDataCommand(command string, arguments []string) error {
	flagSet := flag.NewFlagSet(command, flag.ContinueOnError)
	configPath := flagSet.String("config", "", "configuration path")
	credentialFile := flagSet.String("credential-file", "", "mode-0600 credential file")
	requestID := flagSet.String("request-id", "", "request id")
	addressID := flagSet.String("address-id", "", "address id")
	claimID := flagSet.String("claim-id", "", "claim id")
	ttlMs := flagSet.Uint64("ttl-ms", 0, "ttl ms")
	maxUnits := flagSet.Uint("max-units", 0, "max units")
	maxBytes := flagSet.Uint64("max-bytes", 0, "max bytes")
	visibilityMs := flagSet.Uint64("visibility-timeout-ms", 0, "visibility timeout ms")
	waitMs := flagSet.Uint64("wait-timeout-ms", 0, "wait timeout ms")
	claimEpoch := flagSet.Uint64("claim-epoch", 0, "claim epoch")
	payloadPath := flagSet.String("payload-file", "", "payload file")
	var entries entryList
	flagSet.Var(&entries, "entry", "settle entry unit_id:action, repeatable (1..128)")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}
	var settleEntries []*stationv1.SettleEntry
	if command == "settle" {
		parsed, err := parseSettleEntries(entries)
		if err != nil {
			return err
		}
		settleEntries = parsed
	}
	if *configPath == "" || *credentialFile == "" {
		return errors.New("--config and --credential-file are required")
	}
	config, err := nativeapi.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	token, err := readCredential(*credentialFile)
	if err != nil {
		return err
	}
	client, conn, err := dial(config, token)
	if err != nil {
		return err
	}
	defer conn.Close()

	deadline := 5 * time.Second
	if command == "claim" {
		deadline = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(withAuth(context.Background(), token), deadline)
	defer cancel()

	switch command {
	case "reserve":
		resp, err := client.Reserve(ctx, &stationv1.ReserveRequest{
			RequestId: *requestID,
			AddressId: optionalString(*addressID),
			TtlMs:     *ttlMs,
		})
		if err != nil {
			return err
		}
		fmt.Printf("address_id=%s expires_at_unix_ms=%d created=%t\n", resp.AddressId, resp.ExpiresAtUnixMs, resp.Created)
	case "submit":
		payload, err := readOptionalPayload(*payloadPath)
		if err != nil {
			return err
		}
		resp, err := client.Submit(ctx, &stationv1.SubmitRequest{
			RequestId: *requestID,
			AddressId: *addressID,
			Payload:   payload,
			TtlMs:     *ttlMs,
		})
		if err != nil {
			return err
		}
		fmt.Printf("unit_id=%s accepted_at_unix_ms=%d expires_at_unix_ms=%d\n", resp.UnitId, resp.AcceptedAtUnixMs, resp.ExpiresAtUnixMs)
	case "claim":
		resp, err := client.Claim(ctx, &stationv1.ClaimRequest{
			RequestId:           *requestID,
			AddressId:           *addressID,
			MaxUnits:            uint32(*maxUnits),
			MaxBytes:            *maxBytes,
			VisibilityTimeoutMs: *visibilityMs,
			WaitTimeoutMs:       *waitMs,
		})
		if err != nil {
			return err
		}
		if resp.ClaimId == nil {
			fmt.Println("claim_id=")
			return nil
		}
		fmt.Printf("claim_id=%s claim_epoch=%d units=%d\n", *resp.ClaimId, resp.ClaimEpoch, len(resp.Units))
	case "settle":
		_, err := client.Settle(ctx, &stationv1.SettleRequest{
			RequestId:  *requestID,
			ClaimId:    *claimID,
			ClaimEpoch: *claimEpoch,
			Entries:    settleEntries,
		})
		return err
	case "capacity":
		resp, err := client.GetCapacity(ctx, &stationv1.GetCapacityRequest{})
		if err != nil {
			return err
		}
		fmt.Printf("profile_id=%s pressure=%s\n", resp.ProfileId, resp.Pressure.String())
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	return nil
}

func dial(config nativeapi.Config, token string) (stationv1.StationClient, *grpc.ClientConn, error) {
	target := config.Transport.Listen
	options := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4_325_376)),
	}
	if config.Transport.Mode == "unix" {
		socketPath, err := nativeapi.VerifyUnixSocketPath(config.Transport.SocketPath)
		if err != nil {
			return nil, nil, err
		}
		target = "passthrough:///unix-native"
		options = append(
			options,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return nativeapi.DialVerifiedUnix(ctx, socketPath)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	} else {
		tlsConfig, err := credentials.NewClientTLSFromFile(config.Transport.TLSCertificateFile, "")
		if err != nil {
			return nil, nil, err
		}
		options = append(options, grpc.WithTransportCredentials(tlsConfig))
	}
	conn, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, nil, err
	}
	_ = token
	return stationv1.NewStationClient(conn), conn, nil
}

func withAuth(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+strings.TrimSpace(token)))
}

func readCredential(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("credential file must be mode 0600")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

func readOptionalPayload(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("--payload-file is required for submit")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("payload file must be mode 0600")
	}
	return os.ReadFile(path)
}

// entryList accumulates repeatable --entry flags in order.
type entryList []string

func (list *entryList) String() string { return strings.Join(*list, ",") }

func (list *entryList) Set(value string) error {
	*list = append(*list, value)
	return nil
}

type uint64List []uint64

func (list *uint64List) String() string {
	values := make([]string, len(*list))
	for index, value := range *list {
		values[index] = strconv.FormatUint(value, 10)
	}
	return strings.Join(values, ",")
}

func (list *uint64List) Set(value string) error {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return errors.New("member id must be positive")
	}
	*list = append(*list, parsed)
	return nil
}

const maxSettleEntries = 128

// parseSettleEntries validates the frozen --entry contract (C-09): at least
// one entry, at most 128, each parsed as unit_id:action. COMPLETE carries zero
// delay and zero extension by construction.
func parseSettleEntries(entries []string) ([]*stationv1.SettleEntry, error) {
	if len(entries) == 0 {
		return nil, errors.New("at least one --entry required")
	}
	if len(entries) > maxSettleEntries {
		return nil, fmt.Errorf("at most %d --entry flags allowed", maxSettleEntries)
	}
	settleEntries := make([]*stationv1.SettleEntry, 0, len(entries))
	for _, raw := range entries {
		unitID, action, ok := strings.Cut(raw, ":")
		if !ok || unitID == "" || action == "" {
			return nil, fmt.Errorf("invalid entry %q: want unit_id:action", raw)
		}
		protoAction, ok := settleEntryActions[action]
		if !ok {
			return nil, fmt.Errorf("invalid entry %q: want unit_id:action", raw)
		}
		settleEntries = append(settleEntries, &stationv1.SettleEntry{UnitId: unitID, Action: protoAction})
	}
	return settleEntries, nil
}

// settleEntryActions maps the frozen ctl action names to the Station settle
// actions. "attempt-limit" is accepted as a client synonym for RELEASE: the
// stationcore state machine produces the ATTEMPT_LIMIT outcome only from a
// RELEASE action on an attempt-exhausted unit (C-09).
var settleEntryActions = map[string]stationv1.SettleAction{
	"complete":      stationv1.SettleAction_SETTLE_ACTION_COMPLETE,
	"release":       stationv1.SettleAction_SETTLE_ACTION_RELEASE,
	"extend":        stationv1.SettleAction_SETTLE_ACTION_EXTEND,
	"attempt-limit": stationv1.SettleAction_SETTLE_ACTION_RELEASE,
}

func optionalString(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	copy := value
	return &copy
}

func readStdinCredential() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
