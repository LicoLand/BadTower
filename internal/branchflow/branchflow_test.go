package branchflow

import "testing"

func TestEvaluate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		expected bool
		base     string
		head     string
	}{
		{"temporary to nightly", true, "nightly", "agent/security-review"},
		{"nightly to stable", true, "stable", "nightly"},
		{"stable to release", true, "release", "stable"},
		{"stable to nightly", false, "nightly", "stable"},
		{"temporary to stable", false, "stable", "agent/security-review"},
		{"nightly to release", false, "release", "nightly"},
		{"temporary to retired main", false, "main", "agent/security-review"},
		{"retired main to nightly", false, "nightly", "main"},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := Evaluate(Evaluation{
				EventName: "pull_request",
				BaseRef:   testCase.base,
				HeadRef:   testCase.head,
				Event:     sameRepositoryEvent(),
			})
			if result.OK != testCase.expected {
				t.Fatalf("Evaluate = %#v", result)
			}
		})
	}
	missing := Evaluate(Evaluation{
		EventName: "pull_request",
		BaseRef:   "nightly",
		HeadRef:   "agent/security-review",
	})
	if missing.OK {
		t.Fatal("missing repository identity accepted")
	}
	cross := sameRepositoryEvent()
	cross.PullRequest.Head.Repo.FullName = "fork/repository"
	if Evaluate(Evaluation{
		EventName: "pull_request",
		BaseRef:   "nightly",
		HeadRef:   "agent/security-review",
		Event:     cross,
	}).OK {
		t.Fatal("cross-repository source accepted")
	}
}

func TestVerifyTopology(t *testing.T) {
	t.Parallel()
	tips := map[string]string{
		"nightly": "nightly-tip",
		"stable":  "stable-tip",
		"release": "release-tip",
	}
	cases := []struct {
		name     string
		expected bool
		branch   string
		parents  []string
	}{
		{"temporary merge", true, "nightly", []string{"before", "feature-tip"}},
		{"nightly promotion", true, "stable", []string{"before", "nightly-tip"}},
		{"stable promotion", true, "release", []string{"before", "stable-tip"}},
		{"direct commit", false, "nightly", []string{"before"}},
		{"wrong stable source", false, "stable", []string{"before", "feature-tip"}},
		{"wrong release source", false, "release", []string{"before", "nightly-tip"}},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := VerifyTopology(TopologyInput{
				Branch: testCase.branch,
				Before: "before",
				After:  "after",
				Parents: func(string) ([]string, error) {
					return testCase.parents, nil
				},
				BranchTip: func(branch string) (string, error) {
					return tips[branch], nil
				},
				IsAncestor: func(ancestor, descendant string) bool {
					return ancestor == descendant
				},
			})
			if result.OK != testCase.expected {
				t.Fatalf("VerifyTopology = %#v", result)
			}
		})
	}
}

func sameRepositoryEvent() Event {
	return Event{
		Repository: Repository{FullName: "example/repository"},
		PullRequest: PullRequest{
			Base: PullRequestRef{Repo: Repository{FullName: "example/repository"}},
			Head: PullRequestRef{Repo: Repository{FullName: "example/repository"}},
		},
	}
}
