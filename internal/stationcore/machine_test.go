package stationcore

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestReplicatedCommandAppliesIdentically(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	left := openTestCore(t, newTestClock(start))
	right := openTestCore(t, newTestClock(start))
	namespace := "ns_RRRRRRRRRRRRRRRRRRRRRR"

	commands := []Command{
		mustCommand(t, opNameReserve, ReserveInput{
			NamespaceID: namespace, RequestID: "rq_machineReserve0001", TTLMs: 3_600_000,
		}, start.UnixMilli(), "sa_AAAAAAAAAAAAAAAAAAAAAA"),
		mustCommand(t, opNameSubmit, SubmitInput{
			NamespaceID: namespace, RequestID: "rq_machineSubmit00001",
			AddressID: "sa_AAAAAAAAAAAAAAAAAAAAAA", Payload: []byte("opaque"), TTLMs: 300_000,
		}, start.UnixMilli()+1, "tu_BBBBBBBBBBBBBBBBBBBBBB"),
		mustCommand(t, opNameClaim, ClaimInput{
			NamespaceID: namespace, RequestID: "rq_machineClaim000001",
			AddressID: "sa_AAAAAAAAAAAAAAAAAAAAAA", MaxUnits: 1, MaxBytes: 1024,
			VisibilityTimeoutMs: 30_000,
		}, start.UnixMilli()+2, "dc_CCCCCCCCCCCCCCCCCCCCCC"),
	}

	for _, command := range commands {
		leftResult, leftErr := left.ApplyCommand(context.Background(), command)
		rightResult, rightErr := right.ApplyCommand(context.Background(), command)
		if leftErr != nil || rightErr != nil {
			t.Fatalf("apply %s: left=%v right=%v", command.Operation, leftErr, rightErr)
		}
		if string(leftResult.Payload) != string(rightResult.Payload) || leftResult.Reason != rightResult.Reason {
			t.Fatalf("apply %s diverged: left=%s right=%s", command.Operation, leftResult.Payload, rightResult.Payload)
		}
	}

	leftCapacity, err := left.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	rightCapacity, err := right.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	leftJSON, _ := json.Marshal(leftCapacity)
	rightJSON, _ := json.Marshal(rightCapacity)
	if string(leftJSON) != string(rightJSON) {
		t.Fatalf("capacity diverged: left=%s right=%s", leftJSON, rightJSON)
	}
}

func TestPartitionForIsStableAndBounded(t *testing.T) {
	t.Parallel()
	vectors := map[string]uint16{
		"ns_PPPPPPPPPPPPPPPPPPPPPP": 47,
		"ns_AAAAAAAAAAAAAAAAAAAAAA": 52,
		"ns_BBBBBBBBBBBBBBBBBBBBBB": 14,
		"ns_aaaaaaaaaaaaaaaaaaaaab": 18,
	}
	for namespace, want := range vectors {
		if got := PartitionFor(namespace); got != want {
			t.Errorf("PartitionFor(%q) = %d, want %d", namespace, got, want)
		} else if got >= NamespacePartitions {
			t.Errorf("partition %d outside [0,%d)", got, NamespacePartitions)
		}
	}
}

func TestCommandRejectsPayloadNamespaceMismatch(t *testing.T) {
	t.Parallel()
	outerNamespace := "ns_AAAAAAAAAAAAAAAAAAAAAA"
	payloadNamespace := "ns_BBBBBBBBBBBBBBBBBBBBBB"
	tests := []struct {
		name        string
		operation   string
		generatedID string
		input       any
	}{
		{
			name:        "reserve",
			operation:   opNameReserve,
			generatedID: "sa_AAAAAAAAAAAAAAAAAAAAAA",
			input:       ReserveInput{NamespaceID: payloadNamespace, RequestID: "rq_machineMismatchReserve", TTLMs: 3_600_000},
		},
		{
			name:        "submit",
			operation:   opNameSubmit,
			generatedID: "tu_BBBBBBBBBBBBBBBBBBBBBB",
			input: SubmitInput{
				NamespaceID: payloadNamespace, RequestID: "rq_machineMismatchSubmit",
				AddressID: "sa_AAAAAAAAAAAAAAAAAAAAAA", Payload: []byte("opaque"), TTLMs: 300_000,
			},
		},
		{
			name:        "claim",
			operation:   opNameClaim,
			generatedID: "dc_CCCCCCCCCCCCCCCCCCCCCC",
			input: ClaimInput{
				NamespaceID: payloadNamespace, RequestID: "rq_machineMismatchClaim",
				AddressID: "sa_AAAAAAAAAAAAAAAAAAAAAA", MaxUnits: 1, MaxBytes: 1024,
				VisibilityTimeoutMs: 30_000,
			},
		},
		{
			name:      "settle",
			operation: opNameSettle,
			input: SettleInput{
				NamespaceID: payloadNamespace, RequestID: "rq_machineMismatchSettle",
				ClaimID: "dc_CCCCCCCCCCCCCCCCCCCCCC", ClaimEpoch: 1,
				Entries: []SettleEntry{{UnitID: "tu_BBBBBBBBBBBBBBBBBBBBBB", Action: SettleComplete}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.input)
			if err != nil {
				t.Fatal(err)
			}
			command := Command{
				Version: CommandVersion, Operation: test.operation, NamespaceID: outerNamespace,
				EffectiveNow: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
				GeneratedID:  test.generatedID, OwnershipEpoch: 1, Payload: payload,
			}
			assertInvalidRequest(t, command.Validate())

			core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)))
			result, err := core.ApplyCommand(context.Background(), command)
			assertInvalidRequest(t, err)
			if result.Reason != "" {
				t.Fatalf("pre-apply validation returned result reason %q", result.Reason)
			}
		})
	}
}

func assertInvalidRequest(t *testing.T, err error) {
	t.Helper()
	failure, ok := IsFailure(err)
	if !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("failure = %v, want %s", err, ReasonInvalidRequest)
	}
}

func mustCommand(t *testing.T, operation string, input any, now int64, generatedID string) Command {
	t.Helper()
	command, err := EncodeCommand(operation, input, now, generatedID, 1)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	return command
}
