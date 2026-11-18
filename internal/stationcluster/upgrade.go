package stationcluster

import (
	"errors"
	"slices"
	"sort"
)

var (
	ErrUpgradeAdmission = errors.New("cluster upgrade version admission rejected")
	ErrUpgradeOrder     = errors.New("cluster upgrade order rejected")
	ErrUpgradeQuorum    = errors.New("cluster upgrade would lose quorum")
)

type ClusterVersion struct {
	Command uint32 `json:"commandVersion"`
	Store   uint32 `json:"storeVersion"`
}

func (version ClusterVersion) valid() bool {
	return validControlVersion(version.Command) && validControlVersion(version.Store)
}

type UpgradeStage string

const (
	UpgradeCanaryLearner UpgradeStage = "canary-learner"
	UpgradeFollower      UpgradeStage = "follower"
	UpgradeLeader        UpgradeStage = "leader"
)

type UpgradeStep struct {
	Sequence uint16         `json:"sequence"`
	MemberID uint64         `json:"memberId"`
	Stage    UpgradeStage   `json:"stage"`
	From     ClusterVersion `json:"from"`
	To       ClusterVersion `json:"to"`
}

type UpgradePreflight struct {
	Current              ClusterVersion
	Next                 ClusterVersion
	LeaderMemberIDs      []uint64
	UnavailableMemberIDs []uint64
}

// PlanRollingUpgrade admits exactly N and N+1 and returns the only safe stage
// order: one non-voting canary learner, voters that lead no group, then
// leaders. Each voter step is checked against the currently unavailable set.
func PlanRollingUpgrade(state ControlState, preflight UpgradePreflight) ([]UpgradeStep, error) {
	if state.Validate() != nil || !preflight.Current.valid() || !preflight.Next.valid() ||
		preflight.Next.Command != preflight.Current.Command+1 || preflight.Next.Store != preflight.Current.Store+1 ||
		state.MinCommandVersion > preflight.Current.Command || state.MinStoreVersion > preflight.Current.Store {
		return nil, ErrUpgradeAdmission
	}
	leaders, err := checkedMemberSet(state, preflight.LeaderMemberIDs, false)
	if err != nil || len(leaders) == 0 {
		return nil, ErrUpgradeOrder
	}
	unavailable, err := checkedMemberSet(state, preflight.UnavailableMemberIDs, true)
	if err != nil {
		return nil, ErrUpgradeQuorum
	}

	var learners, followers, leaderMembers []ControlMember
	voterCount := 0
	for _, member := range state.Members {
		version := ClusterVersion{Command: member.CommandVersion, Store: member.StoreVersion}
		if version != preflight.Current && version != preflight.Next {
			return nil, ErrUpgradeAdmission
		}
		if member.Role == ControlRoleLearner {
			if _, isLeader := leaders[member.ID]; isLeader || member.Draining || (member.Phase != ControlPhaseJoining && member.Phase != ControlPhaseCaughtUp) {
				return nil, ErrUpgradeOrder
			}
			learners = append(learners, member)
			continue
		}
		voterCount++
		if _, isLeader := leaders[member.ID]; isLeader {
			leaderMembers = append(leaderMembers, member)
		} else {
			followers = append(followers, member)
		}
	}
	if len(learners) != 1 || len(followers) == 0 || len(leaderMembers) == 0 {
		return nil, ErrUpgradeOrder
	}
	quorum := voterCount/2 + 1
	available := voterCount
	for memberID := range unavailable {
		if member := controlMember(&state, memberID); member != nil && member.Role == ControlRoleVoter {
			available--
		}
	}
	if available < quorum {
		return nil, ErrUpgradeQuorum
	}

	sort.Slice(followers, func(i, j int) bool { return followers[i].ID < followers[j].ID })
	sort.Slice(leaderMembers, func(i, j int) bool { return leaderMembers[i].ID < leaderMembers[j].ID })
	ordered := make([]struct {
		member ControlMember
		stage  UpgradeStage
	}, 0, len(state.Members))
	ordered = append(ordered, struct {
		member ControlMember
		stage  UpgradeStage
	}{learners[0], UpgradeCanaryLearner})
	for _, member := range followers {
		ordered = append(ordered, struct {
			member ControlMember
			stage  UpgradeStage
		}{member, UpgradeFollower})
	}
	for _, member := range leaderMembers {
		ordered = append(ordered, struct {
			member ControlMember
			stage  UpgradeStage
		}{member, UpgradeLeader})
	}

	steps := make([]UpgradeStep, 0, len(ordered))
	for _, candidate := range ordered {
		from := ClusterVersion{Command: candidate.member.CommandVersion, Store: candidate.member.StoreVersion}
		if from == preflight.Next {
			continue
		}
		if candidate.member.Role == ControlRoleVoter {
			_, alreadyUnavailable := unavailable[candidate.member.ID]
			remaining := available
			if !alreadyUnavailable {
				remaining--
			}
			if remaining < quorum {
				return nil, ErrUpgradeQuorum
			}
		}
		steps = append(steps, UpgradeStep{
			Sequence: uint16(len(steps) + 1), MemberID: candidate.member.ID, Stage: candidate.stage,
			From: from, To: preflight.Next,
		})
	}
	return steps, nil
}

// PreflightVersionFloorAdvance returns the replicated command that makes N+1
// the rollback floor only after every member is already at or above it.
func PreflightVersionFloorAdvance(state ControlState, target ClusterVersion) (ControlCommand, error) {
	if state.Validate() != nil || !target.valid() || target.Command < state.MinCommandVersion || target.Store < state.MinStoreVersion {
		return ControlCommand{}, ErrUpgradeAdmission
	}
	for _, member := range state.Members {
		if member.CommandVersion < target.Command || member.StoreVersion < target.Store {
			return ControlCommand{}, ErrUpgradeOrder
		}
	}
	return ControlCommand{
		CommandVersion: target.Command, StoreVersion: target.Store, Kind: ControlSetVersionFloor,
		ExpectedEpoch: state.PlacementEpoch, MinCommand: target.Command, MinStore: target.Store,
	}, nil
}

// PreflightRollback rejects an old binary as soon as either replicated floor
// has advanced beyond it. It does not mutate control state.
func PreflightRollback(state ControlState, target ClusterVersion, memberID uint64) error {
	if state.Validate() != nil || !target.valid() || memberIndex(state.Members, memberID) < 0 {
		return ErrUpgradeAdmission
	}
	if target.Command < state.MinCommandVersion || target.Store < state.MinStoreVersion {
		return ErrVersionRollback
	}
	return nil
}

func checkedMemberSet(state ControlState, ids []uint64, allowEmpty bool) (map[uint64]struct{}, error) {
	if !allowEmpty && len(ids) == 0 {
		return nil, ErrUpgradeOrder
	}
	result := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 || memberIndex(state.Members, id) < 0 || slices.ContainsFunc(state.Members, func(member ControlMember) bool {
			return member.ID == id && member.Role != ControlRoleVoter
		}) {
			return nil, ErrUpgradeOrder
		}
		if _, duplicate := result[id]; duplicate {
			return nil, ErrUpgradeOrder
		}
		result[id] = struct{}{}
	}
	return result, nil
}
