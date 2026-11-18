//go:build station_cluster_acceptance

package acceptance

import (
	"encoding/json"
	"testing"
)

const clusterEvidenceSchema = "badtower.cluster-capacity-evidence.v1"

// clusterLifecycleEvidence deliberately contains only aggregate, profile-bound
// values. It must never acquire topology addresses, roots, credentials,
// namespace/address/unit identifiers, payloads, or raw runtime rows.
type clusterLifecycleEvidence struct {
	Schema                   string   `json:"schema"`
	ProfileID                string   `json:"profileId"`
	Mode                     string   `json:"mode"`
	MembersObserved          uint32   `json:"membersObserved"`
	Partitions               uint16   `json:"partitions"`
	NativeMutations          uint64   `json:"nativeMutations"`
	ColdNamespaceCompletions uint64   `json:"coldNamespaceCompletions"`
	RebalanceMoves           uint32   `json:"rebalanceMoves"`
	BackupPartitions         uint16   `json:"backupPartitions"`
	RestoredPartitions       uint16   `json:"restoredPartitions"`
	CommittedWriteLoss       uint64   `json:"committedWriteLoss"`
	FailoverRTOMillis        int64    `json:"failoverRtoMillis"`
	Scenarios                []string `json:"scenarios"`
}

func (evidence clusterLifecycleEvidence) validate(profile clusterCapacityProfile) error {
	if evidence.Schema != clusterEvidenceSchema || evidence.ProfileID != profile.ProfileID || evidence.Mode != "focused" ||
		evidence.MembersObserved != uint32(profile.Topology.MaximumMembers) || evidence.Partitions != uint16(profile.Topology.Partitions) ||
		evidence.NativeMutations < uint64(profile.Focused.MinimumNativeMutations) ||
		evidence.ColdNamespaceCompletions < uint64(profile.Focused.MinimumColdNamespaceCompletions) ||
		evidence.CommittedWriteLoss != uint64(profile.Focused.CommittedWriteRPO) ||
		evidence.FailoverRTOMillis < 0 || evidence.FailoverRTOMillis > int64(profile.Focused.MaximumFailoverRTOMilliseconds) ||
		evidence.RebalanceMoves == 0 || evidence.BackupPartitions != uint16(profile.Topology.Partitions) || evidence.RestoredPartitions != uint16(profile.Topology.Partitions) ||
		!sameClosedScenarioSet(evidence.Scenarios, profile.Focused.RequiredScenarios) {
		return errInvalidClusterEvidence
	}
	return nil
}

func emitClusterLifecycleEvidence(t *testing.T, evidence clusterLifecycleEvidence) {
	t.Helper()
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cluster lifecycle aggregate evidence: %s", raw)
}

func sameClosedScenarioSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]struct{}, len(got))
	for _, scenario := range got {
		if scenario == "" {
			return false
		}
		seen[scenario] = struct{}{}
	}
	if len(seen) != len(got) {
		return false
	}
	for _, scenario := range want {
		if _, ok := seen[scenario]; !ok {
			return false
		}
	}
	return true
}
