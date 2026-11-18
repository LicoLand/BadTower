package stationcluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

type Node struct {
	id            uint64
	clusterID     string
	config        NodeConfig
	control       ControlRaftNode
	controlDB     *ControlMachine
	members       [PartitionCount]*RaftMember
	cores         [PartitionCount]*stationcore.Core
	stations      [PartitionCount]*MemberStation
	placements    [PartitionCount]Placement
	peerClient    *nodePeerClient
	peerServer    *nodePeerServer
	serverTLS     *tls.Config
	snapshotStore *streamSnapshotStore
	storagePool   *stationcore.StoragePool
	closeOnce     sync.Once
}

const StoreVersion uint32 = 2

func OpenNode(config NodeConfig, serverTLS, clientTLS *tls.Config, clock stationcore.Clock) (*Node, error) {
	validated, err := ValidateNodeConfig(config)
	if err != nil || serverTLS == nil || clientTLS == nil || clock == nil {
		return nil, ErrInvalidTopology
	}
	snapshotStore, err := openStreamSnapshotStore(validated.RaftRoot)
	if err != nil {
		return nil, err
	}
	client, err := newNodePeerClient(validated.ClusterID, validated.Members, clientTLS, snapshotStore)
	if err != nil {
		return nil, err
	}
	bootstrapMembers := validated.BootstrapMembers
	if len(bootstrapMembers) == 0 {
		bootstrapMembers = validated.Members
	}
	topology := Topology{ClusterID: validated.ClusterID, Epoch: validated.OwnershipEpoch, Members: make([]Member, 0, MinMembers)}
	peerIDs := make([]uint64, 0, len(validated.Members))
	for _, configured := range validated.Members {
		peerIDs = append(peerIDs, configured.ID)
	}
	initialPeerIDs := make([]uint64, 0, MinMembers)
	for _, configured := range bootstrapMembers {
		topology.Members = append(topology.Members, Member{ID: configured.ID, FaultDomain: configured.FaultDomain, Incarnation: 1})
		initialPeerIDs = append(initialPeerIDs, configured.ID)
	}
	placements, err := topology.Placements()
	if err != nil {
		client.close()
		return nil, err
	}
	node := &Node{id: validated.MemberID, clusterID: validated.ClusterID, config: validated, peerClient: client, serverTLS: serverTLS.Clone(), snapshotStore: snapshotStore, storagePool: stationcore.NewStoragePool()}
	copy(node.placements[:], placements)
	node.peerServer = &nodePeerServer{node: node}
	controlMembers := make([]ControlMember, 0, len(bootstrapMembers))
	for _, configured := range bootstrapMembers {
		controlMembers = append(controlMembers, ControlMember{
			ID: configured.ID, FaultDomain: configured.FaultDomain, Incarnation: 1,
			CommandVersion: 1, StoreVersion: StoreVersion,
		})
	}
	node.controlDB, err = OpenControlMachine(ControlBootstrap{
		Root: filepath.Join(validated.CoreRoot, "control"), ClusterID: validated.ClusterID,
		PlacementEpoch: validated.OwnershipEpoch, MinCommandVersion: 1, MinStoreVersion: StoreVersion,
		Members: controlMembers,
	})
	if err != nil {
		node.closeOpened(0)
		return nil, err
	}
	controlState := node.controlDB.Snapshot()
	if controlState.MinStoreVersion != StoreVersion {
		node.closeOpened(0)
		return nil, ErrInvalidTopology
	}
	for _, member := range controlState.Members {
		if member.StoreVersion != StoreVersion {
			node.closeOpened(0)
			return nil, ErrInvalidTopology
		}
	}
	controlInitialIDs := initialPeerIDs
	if validated.Join {
		controlInitialIDs = nil
	}
	node.control, err = OpenDurableControlMember(ControlRaftConfig{
		ClusterID: validated.ClusterID, GroupID: ControlGroupID, ID: validated.MemberID,
		InitialPeerIDs: controlInitialIDs, Bootstrap: !validated.Join, RaftRoot: filepath.Join(validated.RaftRoot, "control"),
		Machine: node.controlDB, Transport: client.group(ControlGroupID), TickInterval: defaultTickInterval,
	})
	if err != nil {
		node.closeOpened(0)
		return nil, err
	}
	for partition := uint16(0); partition < PartitionCount; partition++ {
		core, err := stationcore.Open(stationcore.Config{
			DataRoot: filepath.Join(validated.CoreRoot, fmt.Sprintf("partition-%02d", partition)), ProfileID: stationcore.ProfileSingleNodeGeneralV1,
			StorageMode: stationcore.StorageModePartitionOne, Partition: partition, StoragePool: node.storagePool,
		}, clock)
		if err != nil {
			node.closeOpened(partition)
			return nil, err
		}
		node.cores[partition] = core
		member, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: validated.ClusterID, GroupID: uint64(partition) + 1, ID: validated.MemberID,
			Partition: partition, OwnershipEpoch: validated.OwnershipEpoch, PeerIDs: peerIDs,
			BootstrapPeerIDs: initialPeerIDs,
			RaftRoot:         filepath.Join(validated.RaftRoot, fmt.Sprintf("partition-%02d", partition)),
			Core:             core, Transport: client.group(uint64(partition) + 1), TickInterval: defaultTickInterval,
			Join: validated.Join, SnapshotStore: snapshotStore,
		})
		if err != nil {
			node.closeOpened(partition + 1)
			return nil, err
		}
		node.members[partition] = member
		station, err := NewMemberStation(member, core, validated.OwnershipEpoch)
		if err != nil {
			node.closeOpened(partition + 1)
			return nil, err
		}
		node.stations[partition] = station
	}
	return node, nil
}

func OpenNodeFromConfig(config NodeConfig) (*Node, error) {
	serverTLS, clientTLS, err := LoadNodeTLS(config)
	if err != nil {
		return nil, err
	}
	return OpenNode(config, serverTLS, clientTLS, nodeClock{start: time.Now()})
}

func (node *Node) ID() uint64      { return node.id }
func (node *Node) Router() *Router { return &Router{node: node} }

func (node *Node) ServePeer(listener net.Listener) error {
	return node.peerServer.serve(listener, node.serverTLS)
}

func (node *Node) ServePeerContext(ctx context.Context, listener net.Listener) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = node.peerServer.close(shutdownCtx)
			cancel()
		case <-done:
		}
	}()
	err := node.ServePeer(listener)
	close(done)
	return err
}

func (node *Node) CampaignAll(ctx context.Context) error {
	if err := node.control.Campaign(ctx); err != nil && err != ErrNotLeader {
		return err
	}
	for _, member := range node.members {
		if err := member.Campaign(ctx); err != nil && err != ErrNotLeader {
			return err
		}
	}
	return nil
}

func (node *Node) Close() error {
	var result error
	node.closeOnce.Do(func() {
		for _, core := range node.cores {
			if core != nil {
				core.BeginDrain()
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := node.peerServer.close(ctx); err != nil {
			result = err
		}
		for _, member := range node.members {
			if member != nil {
				if err := member.Close(); err != nil && result == nil {
					result = err
				}
			}
		}
		if node.control != nil {
			if err := node.control.Close(); err != nil && result == nil {
				result = err
			}
		}
		for _, core := range node.cores {
			if core != nil {
				if err := core.Close(); err != nil && result == nil {
					result = err
				}
			}
		}
		if node.controlDB != nil {
			if err := node.controlDB.Close(); err != nil && result == nil {
				result = err
			}
		}
		node.peerClient.close()
		if node.storagePool != nil {
			if err := node.storagePool.Close(); err != nil && result == nil {
				result = err
			}
		}
	})
	return result
}

func (node *Node) closeOpened(count uint16) {
	for partition := uint16(0); partition < count && partition < PartitionCount; partition++ {
		if node.members[partition] != nil {
			_ = node.members[partition].Close()
		}
		if node.cores[partition] != nil {
			_ = node.cores[partition].Close()
		}
	}
	if node.control != nil {
		_ = node.control.Close()
	}
	if node.controlDB != nil {
		_ = node.controlDB.Close()
	}
	node.peerClient.close()
	if node.storagePool != nil {
		_ = node.storagePool.Close()
	}
}
