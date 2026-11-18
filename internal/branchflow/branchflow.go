// Package branchflow owns BadTower's protected branch-promotion policy.
package branchflow

import "strings"

const ZeroOID = "0000000000000000000000000000000000000000"

var (
	LongLived = map[string]struct{}{
		"nightly": {},
		"stable":  {},
		"release": {},
	}
	retired  = map[string]struct{}{"main": {}, "master": {}}
	upstream = map[string]string{"stable": "nightly", "release": "stable"}
)

type Repository struct {
	FullName string `json:"full_name"`
}

type PullRequestRef struct {
	Ref  string     `json:"ref"`
	Repo Repository `json:"repo"`
}

type PullRequest struct {
	Base PullRequestRef `json:"base"`
	Head PullRequestRef `json:"head"`
}

type Event struct {
	Repository  Repository  `json:"repository"`
	PullRequest PullRequest `json:"pull_request"`
	Before      string      `json:"before"`
	After       string      `json:"after"`
}

type Decision struct {
	OK   bool
	Code string
}

type Evaluation struct {
	EventName string
	RefName   string
	BaseRef   string
	HeadRef   string
	Event     Event
}

func Evaluate(input Evaluation) Decision {
	if input.EventName == "push" {
		_, governed := LongLived[input.RefName]
		if governed {
			return Decision{OK: true, Code: "protected-push-event"}
		}
		return Decision{Code: "unexpected-push-ref"}
	}
	if input.EventName != "pull_request" && input.EventName != "pull_request_target" {
		return Decision{OK: true, Code: "event-not-governed"}
	}
	base := firstNonEmpty(input.BaseRef, input.Event.PullRequest.Base.Ref)
	head := firstNonEmpty(input.HeadRef, input.Event.PullRequest.Head.Ref)
	if _, isRetired := retired[base]; isRetired {
		return Decision{Code: "retired-base"}
	}
	if _, governed := LongLived[base]; !governed {
		return Decision{OK: true, Code: "base-not-governed"}
	}
	baseRepository := input.Event.Repository.FullName
	headRepository := input.Event.PullRequest.Head.Repo.FullName
	if baseRepository == "" || headRepository == "" || baseRepository != headRepository {
		return Decision{Code: "cross-repository-promotion"}
	}
	if base == "nightly" {
		_, longLived := LongLived[head]
		_, isRetired := retired[head]
		if head != "" && !longLived && !isRetired {
			return Decision{OK: true, Code: "temporary-to-nightly"}
		}
		return Decision{Code: "nightly-source-invalid"}
	}
	required := upstream[base]
	if head == required {
		return Decision{OK: true, Code: required + "-to-" + base}
	}
	return Decision{Code: base + "-source-invalid"}
}

type TopologyInput struct {
	Branch     string
	Before     string
	After      string
	Parents    func(commit string) ([]string, error)
	BranchTip  func(branch string) (string, error)
	IsAncestor func(ancestor, descendant string) bool
}

func VerifyTopology(input TopologyInput) Decision {
	if _, governed := LongLived[input.Branch]; !governed {
		return Decision{Code: "protected-branch-invalid"}
	}
	if input.Before == "" || input.After == "" ||
		input.Before == ZeroOID || input.After == ZeroOID {
		return Decision{Code: "protected-branch-bootstrap-forbidden"}
	}
	parents, err := input.Parents(input.After)
	if err != nil || len(parents) != 2 || parents[0] != input.Before {
		return Decision{Code: "protected-branch-not-single-merge"}
	}
	mergedHead := parents[1]
	if input.Branch == "nightly" {
		for _, protected := range []string{"stable", "release"} {
			tip, err := input.BranchTip(protected)
			if err == nil && tip != "" && input.IsAncestor(mergedHead, tip) {
				return Decision{Code: "nightly-source-protected"}
			}
		}
		return Decision{OK: true, Code: "temporary-merge-advanced-nightly"}
	}
	required := upstream[input.Branch]
	tip, err := input.BranchTip(required)
	if err != nil || strings.TrimSpace(tip) == "" {
		return Decision{Code: "promotion-source-missing"}
	}
	if mergedHead == tip {
		return Decision{OK: true, Code: required + "-merge-advanced-" + input.Branch}
	}
	return Decision{Code: "promotion-source-tip-mismatch"}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
