package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/LicoLand/BadTower/internal/branchflow"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "[branch-flow] verification-failed")
		os.Exit(1)
	}
}

func run() error {
	payload, err := readEvent(os.Getenv("GITHUB_EVENT_PATH"))
	if err != nil {
		return err
	}
	result := branchflow.Evaluate(branchflow.Evaluation{
		EventName: os.Getenv("GITHUB_EVENT_NAME"),
		RefName:   os.Getenv("GITHUB_REF_NAME"),
		BaseRef:   os.Getenv("GITHUB_BASE_REF"),
		HeadRef:   os.Getenv("GITHUB_HEAD_REF"),
		Event:     payload,
	})
	fmt.Printf("[branch-flow] %s\n", result.Code)
	if !result.OK {
		return errors.New(result.Code)
	}
	if os.Getenv("GITHUB_EVENT_NAME") != "push" {
		return nil
	}
	topology := branchflow.VerifyTopology(branchflow.TopologyInput{
		Branch:     os.Getenv("GITHUB_REF_NAME"),
		Before:     payload.Before,
		After:      firstNonEmpty(payload.After, os.Getenv("GITHUB_SHA")),
		Parents:    parents,
		BranchTip:  branchTip,
		IsAncestor: isAncestor,
	})
	fmt.Printf("[branch-flow] %s\n", topology.Code)
	if !topology.OK {
		return errors.New(topology.Code)
	}
	return nil
}

func readEvent(path string) (branchflow.Event, error) {
	if path == "" {
		return branchflow.Event{}, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return branchflow.Event{}, err
	}
	var event branchflow.Event
	err = json.Unmarshal(content, &event)
	return event, err
}

func parents(commit string) ([]string, error) {
	output, err := git("show", "-s", "--format=%P", commit)
	if err != nil {
		return nil, err
	}
	return strings.Fields(output), nil
}

func branchTip(branch string) (string, error) {
	for _, reference := range []string{
		"refs/remotes/origin/" + branch,
		"refs/heads/" + branch,
	} {
		output, err := git("rev-parse", "--verify", reference+"^{commit}")
		if err == nil {
			return strings.TrimSpace(output), nil
		}
	}
	return "", errors.New("branch tip missing")
}

func isAncestor(ancestor, descendant string) bool {
	command := exec.Command("git", "merge-base", "--is-ancestor", ancestor, descendant)
	return command.Run() == nil
}

func git(arguments ...string) (string, error) {
	output, err := exec.Command("git", arguments...).Output()
	return string(output), err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
