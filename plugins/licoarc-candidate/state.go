package licoarccandidate

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const adapterStoreIdentity = "badtower.licoarc-candidate.state.v1"

type stateStore struct {
	mu           sync.Mutex
	db           *pebble.DB
	logicalBytes uint64
	maxBytes     uint64
}

type persistedHandle struct {
	NativeAddressID  string      `json:"nativeAddressId"`
	Class            HandleClass `json:"class"`
	ServiceUntil     uint64      `json:"serviceUntil"`
	Consumed         bool        `json:"consumed"`
	PendingOperation string      `json:"pendingOperation,omitempty"`
}

type persistedAffiliation struct {
	StationDescriptorDigest string `json:"stationDescriptorDigest"`
	AffiliationCommitment   string `json:"affiliationCommitment"`
	NotAfter                uint64 `json:"notAfter"`
}

type persistedAffiliationOperation struct {
	Fingerprint string `json:"fingerprint"`
	NotAfter    uint64 `json:"notAfter"`
	ReplayUntil int64  `json:"replayUntilUnixMs"`
}

type persistedNativeOperation struct {
	Operation   string `json:"operation"`
	Fingerprint string `json:"fingerprint"`
	RequestID   string `json:"requestId"`
	ExpiresAtMS int64  `json:"expiresAtUnixMs"`
}

type persistedReservation struct {
	Handle      string `json:"handle"`
	Fingerprint string `json:"fingerprint"`
}

type persistedClaim struct {
	Handle        string          `json:"handle"`
	NativeClaimID string          `json:"nativeClaimId"`
	NativeEpoch   uint64          `json:"nativeEpoch"`
	ExpiresAtMS   int64           `json:"expiresAtUnixMs"`
	Items         []persistedItem `json:"items"`
}

type persistedItem struct {
	ItemID       string `json:"itemId"`
	NativeUnitID string `json:"nativeUnitId"`
	Packet       []byte `json:"packet"`
}

type persistedResponse struct {
	Fingerprint string      `json:"fingerprint"`
	Status      int         `json:"status"`
	Headers     http.Header `json:"headers"`
	Body        []byte      `json:"body"`
	ExpiresAtMS int64       `json:"expiresAtUnixMs"`
}

func openStateStore(root string, maxBytes uint64, binding [32]byte) (*stateStore, error) {
	if root == "" || !filepath.IsAbs(root) || maxBytes == 0 || maxBytes > MaxPluginStateBytes {
		return nil, ErrInvalidControl
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, ErrInvalidControl
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, ErrInvalidControl
	}
	if len(entries) > 0 {
		pebbleRoot := false
		for _, entry := range entries {
			name := entry.Name()
			if name == "CURRENT" || strings.HasPrefix(name, "MANIFEST") || strings.HasPrefix(name, "OPTIONS") || strings.HasPrefix(name, "LOG") || strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".blob") {
				pebbleRoot = true
				break
			}
		}
		if !pebbleRoot {
			return nil, ErrInvalidControl
		}
	}
	db, err := pebble.Open(root, &pebble.Options{FormatMajorVersion: 13, Logger: privacyStorageLogger{}})
	if err != nil {
		return nil, ErrInvalidControl
	}
	store := &stateStore{db: db, maxBytes: maxBytes}
	value, closer, err := db.Get([]byte("meta/identity"))
	if err == pebble.ErrNotFound {
		batch := db.NewBatch()
		if batch.Set([]byte("meta/identity"), []byte(adapterStoreIdentity), nil) != nil || batch.Set([]byte("meta/binding"), binding[:], nil) != nil || batch.Commit(pebble.Sync) != nil {
			_ = batch.Close()
			_ = db.Close()
			return nil, ErrStateCapacity
		}
		_ = batch.Close()
	} else if err != nil {
		_ = db.Close()
		return nil, ErrInvalidControl
	} else {
		matches := string(value) == adapterStoreIdentity
		closer.Close()
		if !matches {
			_ = db.Close()
			return nil, ErrInvalidControl
		}
		storedBinding, bindingCloser, bindingErr := db.Get([]byte("meta/binding"))
		if bindingErr != nil {
			_ = db.Close()
			return nil, ErrInvalidControl
		}
		matches = bytes.Equal(storedBinding, binding[:])
		bindingCloser.Close()
		if !matches {
			_ = db.Close()
			return nil, ErrInvalidControl
		}
	}
	iterator, err := db.NewIter(nil)
	if err != nil {
		_ = db.Close()
		return nil, ErrInvalidControl
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		store.logicalBytes += uint64(len(iterator.Key()) + len(iterator.Value()))
		if store.logicalBytes > maxBytes {
			_ = iterator.Close()
			_ = db.Close()
			return nil, ErrStateCapacity
		}
	}
	if err := iterator.Close(); err != nil {
		_ = db.Close()
		return nil, ErrInvalidControl
	}
	return store, nil
}

func (store *stateStore) close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return nil
	}
	err := store.db.Close()
	store.db = nil
	return err
}

func (store *stateStore) put(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return ErrInvalidControl
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	previous, exists, err := store.valueSizeLocked(key)
	if err != nil {
		return ErrInvalidControl
	}
	next := store.logicalBytes - previous + uint64(len(raw))
	if !exists {
		next += uint64(len(key))
	}
	if next > store.maxBytes {
		return ErrStateCapacity
	}
	if store.db.Set([]byte(key), raw, pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes = next
	return nil
}

func (store *stateStore) get(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	raw, closer, err := store.db.Get([]byte(key))
	if err != nil {
		return err
	}
	copyRaw := append([]byte(nil), raw...)
	closer.Close()
	decoder := json.NewDecoder(bytes.NewReader(copyRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidControl
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ErrInvalidControl
	}
	return nil
}

func (store *stateStore) delete(key string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	previous, exists, err := store.valueSizeLocked(key)
	if err != nil || !exists || store.db.Delete([]byte(key), pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes -= uint64(len(key)) + previous
	return nil
}

func (store *stateStore) putWithDeadline(key string, value any, expiresAtMS int64) error {
	if expiresAtMS <= 0 {
		return ErrInvalidControl
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return ErrInvalidControl
	}
	deadline := deadlineKey(expiresAtMS, key)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	previous, exists, err := store.valueSizeLocked(key)
	if err != nil {
		return ErrInvalidControl
	}
	_, deadlineExists, err := store.valueSizeLocked(deadline)
	if err != nil {
		return ErrInvalidControl
	}
	next := store.logicalBytes - previous + uint64(len(raw))
	if !exists {
		next += uint64(len(key))
	}
	if !deadlineExists {
		next += uint64(len(deadline))
	}
	if next > store.maxBytes {
		return ErrStateCapacity
	}
	batch := store.db.NewBatch()
	defer batch.Close()
	if batch.Set([]byte(key), raw, nil) != nil || batch.Set([]byte(deadline), nil, nil) != nil || batch.Commit(pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes = next
	return nil
}

func (store *stateStore) deleteWithDeadline(key string, expiresAtMS int64) error {
	deadline := deadlineKey(expiresAtMS, key)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	valueSize, exists, err := store.valueSizeLocked(key)
	if err != nil || !exists {
		return ErrInvalidControl
	}
	_, deadlineExists, err := store.valueSizeLocked(deadline)
	if err != nil {
		return ErrInvalidControl
	}
	batch := store.db.NewBatch()
	defer batch.Close()
	if batch.Delete([]byte(key), nil) != nil || batch.Delete([]byte(deadline), nil) != nil || batch.Commit(pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes -= uint64(len(key)) + valueSize
	if deadlineExists {
		store.logicalBytes -= uint64(len(deadline))
	}
	return nil
}

func (store *stateStore) loadState(handles map[[32]byte]handleState, claims map[[32]byte]claimState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	iterator, err := store.db.NewIter(&pebble.IterOptions{LowerBound: []byte("handle/"), UpperBound: []byte("handle0")})
	if err != nil {
		return ErrInvalidControl
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var persisted persistedHandle
		if json.Unmarshal(iterator.Value(), &persisted) != nil {
			_ = iterator.Close()
			return ErrInvalidControl
		}
		token, ok := parseToken(string(iterator.Key()[len("handle/"):]))
		if !ok {
			_ = iterator.Close()
			return ErrInvalidControl
		}
		handles[token] = handleState(persisted)
	}
	if err := iterator.Close(); err != nil {
		return ErrInvalidControl
	}
	iterator, err = store.db.NewIter(&pebble.IterOptions{LowerBound: []byte("claim/"), UpperBound: []byte("claim0")})
	if err != nil {
		return ErrInvalidControl
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var persisted persistedClaim
		if json.Unmarshal(iterator.Value(), &persisted) != nil {
			_ = iterator.Close()
			return ErrInvalidControl
		}
		claimID, ok := parseToken(string(iterator.Key()[len("claim/"):]))
		if !ok {
			_ = iterator.Close()
			return ErrInvalidControl
		}
		ownerHandle, ok := parseToken(persisted.Handle)
		if !ok {
			_ = iterator.Close()
			return ErrInvalidControl
		}
		claim := claimState{Handle: ownerHandle, NativeClaimID: persisted.NativeClaimID, NativeEpoch: persisted.NativeEpoch, ExpiresAtMS: persisted.ExpiresAtMS, Items: make(map[[32]byte]claimItem, len(persisted.Items))}
		for _, item := range persisted.Items {
			itemID, ok := parseToken(item.ItemID)
			if !ok {
				_ = iterator.Close()
				return ErrInvalidControl
			}
			claim.Items[itemID] = claimItem{NativeUnitID: item.NativeUnitID, Packet: append([]byte(nil), item.Packet...)}
		}
		claims[claimID] = claim
	}
	return iterator.Close()
}

func (store *stateStore) putHandle(handle [32]byte, state handleState) error {
	return store.putWithDeadline("handle/"+tokenString(handle), persistedHandle(state), int64(state.ServiceUntil)*1000)
}

func (store *stateStore) putReservedHandle(operationID [16]byte, fingerprint [32]byte, handle [32]byte, state handleState, reservationExpiresMS int64) error {
	stateRaw, err := json.Marshal(persistedHandle(state))
	if err != nil {
		return ErrInvalidControl
	}
	reservationRaw, err := json.Marshal(persistedReservation{Handle: tokenString(handle), Fingerprint: hex.EncodeToString(fingerprint[:])})
	if err != nil {
		return ErrInvalidControl
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	handleKey := "handle/" + tokenString(handle)
	reservationKey := "reservation/" + hex.EncodeToString(operationID[:])
	deadline := deadlineKey(int64(state.ServiceUntil)*1000, handleKey)
	reservationDeadline := deadlineKey(reservationExpiresMS, reservationKey)
	handlePrevious, handleExists, err := store.valueSizeLocked(handleKey)
	if err != nil {
		return ErrInvalidControl
	}
	reservationPrevious, reservationExists, err := store.valueSizeLocked(reservationKey)
	if err != nil {
		return ErrInvalidControl
	}
	next := store.logicalBytes - handlePrevious - reservationPrevious + uint64(len(stateRaw)+len(reservationRaw))
	if !handleExists {
		next += uint64(len(handleKey))
	}
	if !reservationExists {
		next += uint64(len(reservationKey))
	}
	_, deadlineExists, err := store.valueSizeLocked(deadline)
	if err != nil {
		return ErrInvalidControl
	}
	if !deadlineExists {
		next += uint64(len(deadline))
	}
	_, reservationDeadlineExists, err := store.valueSizeLocked(reservationDeadline)
	if err != nil {
		return ErrInvalidControl
	}
	if !reservationDeadlineExists {
		next += uint64(len(reservationDeadline))
	}
	if next > store.maxBytes {
		return ErrStateCapacity
	}
	batch := store.db.NewBatch()
	defer batch.Close()
	if batch.Set([]byte(handleKey), stateRaw, nil) != nil || batch.Set([]byte(reservationKey), reservationRaw, nil) != nil || batch.Set([]byte(deadline), nil, nil) != nil || batch.Set([]byte(reservationDeadline), nil, nil) != nil || batch.Commit(pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes = next
	return nil
}

func (store *stateStore) valueSizeLocked(key string) (uint64, bool, error) {
	raw, closer, err := store.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	size := uint64(len(raw))
	closer.Close()
	return size, true, nil
}

func (store *stateStore) getLiveAffiliation(descriptor, commitment [32]byte, now uint64) (uint64, bool, error) {
	fingerprint := affiliationFingerprint(descriptor, commitment)
	var persisted persistedAffiliation
	if err := store.get("affiliation/"+hex.EncodeToString(fingerprint[:]), &persisted); err != nil {
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, ErrInvalidControl
	}
	if persisted.NotAfter <= now {
		return 0, false, nil
	}
	if persisted.StationDescriptorDigest != hex.EncodeToString(descriptor[:]) || persisted.AffiliationCommitment != hex.EncodeToString(commitment[:]) {
		return 0, false, ErrInvalidControl
	}
	return persisted.NotAfter, true, nil
}

func (store *stateStore) getAffiliationOperation(operationID [16]byte, fingerprint [32]byte, nowMS int64) (uint64, bool, error) {
	var persisted persistedAffiliationOperation
	if err := store.get("affiliation-operation/"+hex.EncodeToString(operationID[:]), &persisted); err != nil {
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, ErrInvalidControl
	}
	if nowMS >= persisted.ReplayUntil {
		return 0, false, nil
	}
	if persisted.Fingerprint != hex.EncodeToString(fingerprint[:]) || persisted.NotAfter == 0 || persisted.ReplayUntil <= 0 || persisted.NotAfter > uint64((1<<63-1)/1000) || persisted.ReplayUntil > int64(persisted.NotAfter)*1000 {
		return 0, false, ErrInvalidControl
	}
	return persisted.NotAfter, true, nil
}

func (store *stateStore) putAffiliation(operationID [16]byte, fingerprint [32]byte, request AffiliateRequest, notAfter uint64, replayUntilMS int64) error {
	if notAfter == 0 || notAfter > uint64((1<<63-1)/1000) || replayUntilMS <= 0 || replayUntilMS > int64(notAfter)*1000 {
		return ErrInvalidControl
	}
	affiliation := persistedAffiliation{
		StationDescriptorDigest: hex.EncodeToString(request.StationDescriptorDigest[:]),
		AffiliationCommitment:   hex.EncodeToString(request.AffiliationCommitment[:]),
		NotAfter:                notAfter,
	}
	operation := persistedAffiliationOperation{
		Fingerprint: hex.EncodeToString(fingerprint[:]), NotAfter: notAfter, ReplayUntil: replayUntilMS,
	}
	affiliationRaw, err := json.Marshal(affiliation)
	if err != nil {
		return ErrInvalidControl
	}
	operationRaw, err := json.Marshal(operation)
	if err != nil {
		return ErrInvalidControl
	}
	type timedRecord struct {
		key       string
		raw       []byte
		expiresAt int64
		oldExpiry func([]byte) (int64, error)
	}
	records := []timedRecord{
		{
			key: "affiliation/" + hex.EncodeToString(fingerprint[:]), raw: affiliationRaw, expiresAt: int64(notAfter) * 1000,
			oldExpiry: func(raw []byte) (int64, error) {
				var value persistedAffiliation
				if json.Unmarshal(raw, &value) != nil || value.NotAfter == 0 || value.NotAfter > uint64((1<<63-1)/1000) {
					return 0, ErrInvalidControl
				}
				return int64(value.NotAfter) * 1000, nil
			},
		},
		{
			key: "affiliation-operation/" + hex.EncodeToString(operationID[:]), raw: operationRaw, expiresAt: replayUntilMS,
			oldExpiry: func(raw []byte) (int64, error) {
				var value persistedAffiliationOperation
				if json.Unmarshal(raw, &value) != nil || value.ReplayUntil <= 0 {
					return 0, ErrInvalidControl
				}
				return value.ReplayUntil, nil
			},
		},
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrInvalidControl
	}
	next := store.logicalBytes
	batch := store.db.NewBatch()
	defer batch.Close()
	for _, record := range records {
		previous, closer, getErr := store.db.Get([]byte(record.key))
		exists := getErr == nil
		if getErr != nil && getErr != pebble.ErrNotFound {
			return ErrInvalidControl
		}
		if exists {
			previousRaw := append([]byte(nil), previous...)
			closer.Close()
			if next < uint64(len(previousRaw)) {
				return ErrInvalidControl
			}
			next -= uint64(len(previousRaw))
			oldExpiry, expiryErr := record.oldExpiry(previousRaw)
			if expiryErr != nil {
				return expiryErr
			}
			oldDeadline := deadlineKey(oldExpiry, record.key)
			newDeadline := deadlineKey(record.expiresAt, record.key)
			if oldDeadline != newDeadline {
				_, oldDeadlineExists, sizeErr := store.valueSizeLocked(oldDeadline)
				if sizeErr != nil {
					return ErrInvalidControl
				}
				if oldDeadlineExists {
					if next < uint64(len(oldDeadline)) || batch.Delete([]byte(oldDeadline), nil) != nil {
						return ErrInvalidControl
					}
					next -= uint64(len(oldDeadline))
				}
			}
		} else {
			next += uint64(len(record.key))
		}
		next += uint64(len(record.raw))
		newDeadline := deadlineKey(record.expiresAt, record.key)
		_, deadlineExists, sizeErr := store.valueSizeLocked(newDeadline)
		if sizeErr != nil {
			return ErrInvalidControl
		}
		if !deadlineExists {
			next += uint64(len(newDeadline))
		}
		if next > store.maxBytes || batch.Set([]byte(record.key), record.raw, nil) != nil || batch.Set([]byte(newDeadline), nil, nil) != nil {
			return ErrStateCapacity
		}
	}
	if batch.Commit(pebble.Sync) != nil {
		return ErrInvalidControl
	}
	store.logicalBytes = next
	return nil
}

func (store *stateStore) nativeRequestID(operation string, operationID [16]byte, fingerprint [32]byte, now time.Time) (string, error) {
	switch operation {
	case "reserve", "submit", "claim", "settle":
	default:
		return "", ErrInvalidControl
	}
	nowMS := now.UnixMilli()
	expiresAtMS := now.Add(IdempotencyWindowSeconds * time.Second).UnixMilli()
	if nowMS <= 0 || expiresAtMS <= nowMS {
		return "", ErrInvalidControl
	}
	key := "native-operation/" + hex.EncodeToString(operationID[:])

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return "", ErrInvalidControl
	}
	var previousRaw []byte
	previous, closer, err := store.db.Get([]byte(key))
	exists := err == nil
	if err != nil && err != pebble.ErrNotFound {
		return "", ErrInvalidControl
	}
	var previousValue persistedNativeOperation
	if exists {
		previousRaw = append([]byte(nil), previous...)
		closer.Close()
		decoder := json.NewDecoder(bytes.NewReader(previousRaw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&previousValue) != nil || decoder.Decode(&struct{}{}) != io.EOF || previousValue.ExpiresAtMS <= 0 || !validNativeID(previousValue.RequestID, "rq_") {
			return "", ErrInvalidControl
		}
		if nowMS < previousValue.ExpiresAtMS {
			if previousValue.Operation != operation || previousValue.Fingerprint != hex.EncodeToString(fingerprint[:]) {
				return "", ErrInvalidControl
			}
			return previousValue.RequestID, nil
		}
	}

	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return "", ErrInvalidControl
	}
	hash := sha256.New()
	hash.Write([]byte("badtower.native-caller-request.v1\x00"))
	hash.Write([]byte(operation))
	hash.Write(operationID[:])
	hash.Write(generation[:])
	requestID := "rq_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil)[:16])
	value := persistedNativeOperation{
		Operation: operation, Fingerprint: hex.EncodeToString(fingerprint[:]),
		RequestID: requestID, ExpiresAtMS: expiresAtMS,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", ErrInvalidControl
	}
	next := store.logicalBytes
	batch := store.db.NewBatch()
	defer batch.Close()
	if exists {
		if next < uint64(len(previousRaw)) {
			return "", ErrInvalidControl
		}
		next -= uint64(len(previousRaw))
		oldDeadline := deadlineKey(previousValue.ExpiresAtMS, key)
		newDeadline := deadlineKey(expiresAtMS, key)
		if oldDeadline != newDeadline {
			_, deadlineExists, sizeErr := store.valueSizeLocked(oldDeadline)
			if sizeErr != nil {
				return "", ErrInvalidControl
			}
			if deadlineExists {
				if next < uint64(len(oldDeadline)) || batch.Delete([]byte(oldDeadline), nil) != nil {
					return "", ErrInvalidControl
				}
				next -= uint64(len(oldDeadline))
			}
		}
	} else {
		next += uint64(len(key))
	}
	next += uint64(len(raw))
	newDeadline := deadlineKey(expiresAtMS, key)
	_, deadlineExists, sizeErr := store.valueSizeLocked(newDeadline)
	if sizeErr != nil {
		return "", ErrInvalidControl
	}
	if !deadlineExists {
		next += uint64(len(newDeadline))
	}
	if next > store.maxBytes {
		return "", ErrStateCapacity
	}
	if batch.Set([]byte(key), raw, nil) != nil || batch.Set([]byte(newDeadline), nil, nil) != nil || batch.Commit(pebble.Sync) != nil {
		return "", ErrInvalidControl
	}
	store.logicalBytes = next
	return requestID, nil
}

func (store *stateStore) getReservation(operationID [16]byte, fingerprint [32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	var persisted persistedReservation
	if err := store.get("reservation/"+hex.EncodeToString(operationID[:]), &persisted); err != nil {
		if isNotFound(err) {
			return zero, false, nil
		}
		return zero, false, ErrInvalidControl
	}
	if persisted.Fingerprint != hex.EncodeToString(fingerprint[:]) {
		return zero, false, ErrInvalidControl
	}
	handle, ok := parseToken(persisted.Handle)
	if !ok {
		return zero, false, ErrInvalidControl
	}
	return handle, true, nil
}

func (store *stateStore) putClaim(claimID [32]byte, claim claimState) error {
	persisted := persistedClaim{Handle: tokenString(claim.Handle), NativeClaimID: claim.NativeClaimID, NativeEpoch: claim.NativeEpoch, ExpiresAtMS: claim.ExpiresAtMS, Items: make([]persistedItem, 0, len(claim.Items))}
	ids := make([][32]byte, 0, len(claim.Items))
	for itemID := range claim.Items {
		ids = append(ids, itemID)
	}
	slices.SortFunc(ids, func(left, right [32]byte) int { return bytes.Compare(left[:], right[:]) })
	for _, itemID := range ids {
		item := claim.Items[itemID]
		persisted.Items = append(persisted.Items, persistedItem{ItemID: tokenString(itemID), NativeUnitID: item.NativeUnitID, Packet: append([]byte(nil), item.Packet...)})
	}
	return store.putWithDeadline("claim/"+tokenString(claimID), persisted, claim.ExpiresAtMS)
}

func (store *stateStore) deleteClaim(claimID [32]byte, expiresAtMS int64) error {
	return store.deleteWithDeadline("claim/"+tokenString(claimID), expiresAtMS)
}

func (store *stateStore) putResponse(operationID [16]byte, response cachedResponse) error {
	persisted := persistedResponse{
		Fingerprint: hex.EncodeToString(response.fingerprint[:]), Status: response.status,
		Headers: response.headers.Clone(), Body: append([]byte(nil), response.body...), ExpiresAtMS: response.expiresAt.UnixMilli(),
	}
	return store.putWithDeadline("response/"+hex.EncodeToString(operationID[:]), persisted, persisted.ExpiresAtMS)
}

func (store *stateStore) getResponse(operationID [16]byte, now time.Time) (cachedResponse, bool) {
	var persisted persistedResponse
	key := "response/" + hex.EncodeToString(operationID[:])
	if err := store.get(key, &persisted); err != nil {
		return cachedResponse{}, false
	}
	if now.UnixMilli() >= persisted.ExpiresAtMS {
		_ = store.deleteWithDeadline(key, persisted.ExpiresAtMS)
		return cachedResponse{}, false
	}
	fingerprint, err := hex.DecodeString(persisted.Fingerprint)
	if err != nil || len(fingerprint) != 32 || persisted.Status < 100 || persisted.Status > 599 {
		return cachedResponse{}, false
	}
	var result cachedResponse
	copy(result.fingerprint[:], fingerprint)
	result.status, result.headers, result.body, result.expiresAt = persisted.Status, persisted.Headers.Clone(), append([]byte(nil), persisted.Body...), time.UnixMilli(persisted.ExpiresAtMS)
	return result, true
}

func (store *stateStore) pruneExpired(nowMS int64, maximum int) ([]string, error) {
	if maximum <= 0 {
		return nil, ErrInvalidControl
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return nil, ErrInvalidControl
	}
	iterator, err := store.db.NewIter(&pebble.IterOptions{LowerBound: []byte("deadline/"), UpperBound: []byte("deadline0")})
	if err != nil {
		return nil, ErrInvalidControl
	}
	defer iterator.Close()
	targets := make([]string, 0, maximum)
	deadlines := make([]string, 0, maximum)
	for iterator.First(); iterator.Valid() && len(targets) < maximum; iterator.Next() {
		key := string(iterator.Key())
		if len(key) < 31 || key[29] != '/' {
			return nil, ErrInvalidControl
		}
		expiresAt, err := strconv.ParseInt(key[9:29], 10, 64)
		if err != nil {
			return nil, ErrInvalidControl
		}
		if expiresAt > nowMS {
			break
		}
		targets = append(targets, key[30:])
		deadlines = append(deadlines, key)
	}
	if len(targets) == 0 {
		return nil, nil
	}
	batch := store.db.NewBatch()
	defer batch.Close()
	removed := uint64(0)
	for index, target := range targets {
		valueSize, exists, err := store.valueSizeLocked(target)
		if err != nil {
			return nil, ErrInvalidControl
		}
		if exists {
			removed += uint64(len(target)) + valueSize
		}
		removed += uint64(len(deadlines[index]))
		if batch.Delete([]byte(target), nil) != nil || batch.Delete([]byte(deadlines[index]), nil) != nil {
			return nil, ErrInvalidControl
		}
	}
	if batch.Commit(pebble.Sync) != nil || removed > store.logicalBytes {
		return nil, ErrInvalidControl
	}
	store.logicalBytes -= removed
	return targets, nil
}

func deadlineKey(expiresAtMS int64, target string) string {
	return fmt.Sprintf("deadline/%020d/%s", expiresAtMS, target)
}

func isNotFound(err error) bool { return errors.Is(err, pebble.ErrNotFound) }

type privacyStorageLogger struct{}

func (privacyStorageLogger) Infof(string, ...interface{})  {}
func (privacyStorageLogger) Errorf(string, ...interface{}) {}
func (privacyStorageLogger) Fatalf(string, ...interface{}) { panic("LICOARC_STATE_FATAL") }
