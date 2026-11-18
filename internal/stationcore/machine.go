package stationcore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
)

const (
	CommandVersion      uint16 = 1
	NamespacePartitions        = 64
	OperationReserve           = opNameReserve
	OperationSubmit            = opNameSubmit
	OperationClaim             = opNameClaim
	OperationSettle            = opNameSettle
)

// Command is the complete deterministic input replicated by a Station
// Cluster. Wall-clock time and generated identifiers are leader-selected and
// carried in the command so every replica applies identical bytes.
type Command struct {
	Version        uint16          `json:"version"`
	Operation      string          `json:"operation"`
	NamespaceID    string          `json:"namespaceId"`
	EffectiveNow   int64           `json:"effectiveNowUnixMs"`
	GeneratedID    string          `json:"generatedId,omitempty"`
	FinalAttempt   bool            `json:"finalAttempt,omitempty"`
	OwnershipEpoch uint64          `json:"ownershipEpoch"`
	Payload        json.RawMessage `json:"payload"`
}

// CommandResult is the deterministic application result. Failure reasons are
// stable Station reasons; raw storage or runtime errors never enter the log.
type CommandResult struct {
	Payload json.RawMessage `json:"payload,omitempty"`
	Reason  Reason          `json:"reason,omitempty"`
}

// PartitionFor deterministically maps one valid namespace to the fixed
// partition set without retaining the namespace in placement state.
func PartitionFor(namespace string) uint16 {
	sum := sha256.Sum256([]byte(namespace))
	return uint16(binary.BigEndian.Uint64(sum[:8]) % NamespacePartitions)
}

func (command Command) Validate() error {
	if command.Version != CommandVersion || command.EffectiveNow < 0 || command.OwnershipEpoch == 0 {
		return fail(ReasonInvalidRequest, false, 0)
	}
	if err := validateNamespace(command.NamespaceID); err != nil {
		return err
	}
	switch command.Operation {
	case opNameReserve, opNameSubmit, opNameClaim, opNameSettle:
	default:
		return fail(ReasonInvalidRequest, false, 0)
	}
	if len(command.Payload) == 0 {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return command.validatePayloadNamespace()
}

func (command Command) validatePayloadNamespace() error {
	var namespace string
	switch command.Operation {
	case opNameReserve:
		var input ReserveInput
		if err := decodeCommandPayload(command.Payload, &input); err != nil {
			return err
		}
		namespace = input.NamespaceID
	case opNameSubmit:
		var input SubmitInput
		if err := decodeCommandPayload(command.Payload, &input); err != nil {
			return err
		}
		namespace = input.NamespaceID
	case opNameClaim:
		var input ClaimInput
		if err := decodeCommandPayload(command.Payload, &input); err != nil {
			return err
		}
		namespace = input.NamespaceID
	case opNameSettle:
		var input SettleInput
		if err := decodeCommandPayload(command.Payload, &input); err != nil {
			return err
		}
		namespace = input.NamespaceID
	default:
		return fail(ReasonInvalidRequest, false, 0)
	}
	if namespace != command.NamespaceID {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

// ApplyCommand is the sole replicated mutation entry point. The existing
// public methods use the same implementations with locally selected time and
// identifiers, preserving one state machine for Instance and Cluster modes.
func (core *Core) ApplyCommand(ctx context.Context, command Command) (CommandResult, error) {
	if err := command.Validate(); err != nil {
		return CommandResult{}, err
	}
	if err := core.validateOwnedNamespace(command.NamespaceID); err != nil {
		return CommandResult{}, err
	}
	directive := &mutationDirective{now: command.EffectiveNow, generatedID: command.GeneratedID, finalAttempt: command.FinalAttempt}
	var value any
	var err error
	switch command.Operation {
	case opNameReserve:
		var input ReserveInput
		if err = decodeCommandPayload(command.Payload, &input); err == nil {
			value, err = core.reserve(ctx, input, directive)
		}
	case opNameSubmit:
		var input SubmitInput
		if err = decodeCommandPayload(command.Payload, &input); err == nil {
			value, err = core.submit(ctx, input, directive)
		}
	case opNameClaim:
		var input ClaimInput
		if err = decodeCommandPayload(command.Payload, &input); err == nil {
			value, err = core.claim(ctx, input, directive)
		}
	case opNameSettle:
		var input SettleInput
		if err = decodeCommandPayload(command.Payload, &input); err == nil {
			value, err = core.settle(ctx, input, directive)
		}
	}
	if err != nil {
		if stationErr, ok := IsFailure(err); ok {
			return CommandResult{Reason: stationErr.Reason}, err
		}
		return CommandResult{Reason: ReasonInternal}, err
	}
	payload, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return CommandResult{}, fail(ReasonInternal, false, 0)
	}
	return CommandResult{Payload: payload}, nil
}

func (core *Core) validateOwnedNamespace(namespace string) error {
	if core.config.StorageMode == StorageModePartitionOne && PartitionFor(namespace) != core.config.Partition {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func EncodeCommand(operation string, input any, effectiveNow int64, generatedID string, ownershipEpoch uint64) (Command, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return Command{}, fail(ReasonInvalidRequest, false, 0)
	}
	var namespace string
	switch typed := input.(type) {
	case ReserveInput:
		namespace = typed.NamespaceID
	case SubmitInput:
		namespace = typed.NamespaceID
	case ClaimInput:
		namespace = typed.NamespaceID
	case SettleInput:
		namespace = typed.NamespaceID
	default:
		return Command{}, fail(ReasonInvalidRequest, false, 0)
	}
	command := Command{
		Version: CommandVersion, Operation: operation, NamespaceID: namespace,
		EffectiveNow: effectiveNow, GeneratedID: generatedID,
		OwnershipEpoch: ownershipEpoch, Payload: payload,
	}
	return command, command.Validate()
}

// PrepareCommand selects all nondeterministic inputs before a proposal enters
// Raft. Followers never read their clocks or entropy to apply the command.
func (core *Core) PrepareCommand(operation string, input any, ownershipEpoch uint64) (Command, error) {
	var prefix string
	switch operation {
	case opNameReserve:
		prefix = idPrefixAddress
	case opNameSubmit:
		prefix = idPrefixUnit
	case opNameClaim:
		prefix = idPrefixClaim
	case opNameSettle:
		return EncodeCommand(operation, input, core.effectiveNowMs(), "", ownershipEpoch)
	default:
		return Command{}, fail(ReasonInvalidRequest, false, 0)
	}
	generatedID, err := generateID(prefix)
	if err != nil {
		return Command{}, fail(ReasonInternal, false, 0)
	}
	return EncodeCommand(operation, input, core.effectiveNowMs(), generatedID, ownershipEpoch)
}

func decodeCommandPayload(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fail(ReasonInvalidRequest, false, 0)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

type mutationDirective struct {
	now          int64
	generatedID  string
	finalAttempt bool
}

func (core *Core) commandNow(directive *mutationDirective) int64 {
	if directive == nil {
		return core.effectiveNowMs()
	}
	core.mu.Lock()
	if directive.now > core.watermark {
		core.watermark = directive.now
	}
	core.mu.Unlock()
	return directive.now
}

func commandGeneratedID(directive *mutationDirective, prefix string) (string, error) {
	if directive == nil {
		return generateID(prefix)
	}
	var err error
	switch prefix {
	case idPrefixAddress:
		err = validateAddressID(directive.generatedID)
	case idPrefixUnit:
		err = validateUnitID(directive.generatedID)
	case idPrefixClaim:
		err = validateClaimID(directive.generatedID)
	default:
		err = fail(ReasonInvalidRequest, false, 0)
	}
	if err != nil {
		return "", err
	}
	return directive.generatedID, nil
}
