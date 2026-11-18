package stationcluster

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

const controlStoreIdentity = "badtower.station-cluster.control-state.v1"

type controlStore struct {
	mu sync.Mutex
	db *pebble.DB
}

func openControlStore(root, clusterID string) (*controlStore, ControlState, bool, error) {
	if !filepath.IsAbs(root) || clusterID == "" || len(clusterID) > 128 {
		return nil, ControlState{}, false, ErrInvalidTopology
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, ControlState{}, false, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, ControlState{}, false, err
	}
	hadExistingStore := len(entries) > 0
	if hadExistingStore {
		pebbleRoot := false
		for _, entry := range entries {
			name := entry.Name()
			if name == "CURRENT" || strings.HasPrefix(name, "MANIFEST") || strings.HasPrefix(name, "OPTIONS") || strings.HasSuffix(name, ".sst") {
				pebbleRoot = true
			}
		}
		if !pebbleRoot {
			return nil, ControlState{}, false, ErrInvalidTopology
		}
	}
	db, err := pebble.Open(root, &pebble.Options{FormatMajorVersion: 13, Logger: controlStorageLogger{}})
	if err != nil {
		return nil, ControlState{}, false, err
	}
	store := &controlStore{db: db}
	identity, identityOK, err := controlRead(db, "meta/identity")
	if err != nil {
		_ = db.Close()
		return nil, ControlState{}, false, err
	}
	if !identityOK {
		if hadExistingStore {
			_ = db.Close()
			return nil, ControlState{}, false, ErrInvalidTopology
		}
		batch := db.NewBatch()
		defer batch.Close()
		if err := batch.Set([]byte("meta/identity"), []byte(controlStoreIdentity), nil); err != nil {
			_ = db.Close()
			return nil, ControlState{}, false, err
		}
		if err := batch.Set([]byte("meta/cluster-id"), []byte(clusterID), nil); err != nil {
			_ = db.Close()
			return nil, ControlState{}, false, err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = db.Close()
			return nil, ControlState{}, false, err
		}
		return store, ControlState{}, true, nil
	}
	boundCluster, clusterOK, err := controlRead(db, "meta/cluster-id")
	if err != nil || string(identity) != controlStoreIdentity || !clusterOK || string(boundCluster) != clusterID {
		_ = db.Close()
		return nil, ControlState{}, false, ErrInvalidTopology
	}
	raw, snapshotOK, err := controlRead(db, "state/snapshot")
	if err != nil {
		_ = db.Close()
		return nil, ControlState{}, false, err
	}
	if !snapshotOK {
		_ = db.Close()
		return nil, ControlState{}, false, ErrInvalidTopology
	}
	var snapshot ControlState
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil || snapshot.ClusterID != clusterID {
		_ = db.Close()
		return nil, ControlState{}, false, ErrInvalidTopology
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		_ = db.Close()
		return nil, ControlState{}, false, ErrInvalidTopology
	}
	return store, snapshot, false, nil
}

func (store *controlStore) save(state ControlState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.db.Set([]byte("state/snapshot"), raw, pebble.Sync)
}

func (store *controlStore) close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.db.Close()
}

func controlRead(db *pebble.DB, key string) ([]byte, bool, error) {
	raw, closer, err := db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

type controlStorageLogger struct{}

func (controlStorageLogger) Infof(string, ...interface{})  {}
func (controlStorageLogger) Errorf(string, ...interface{}) {}
func (controlStorageLogger) Fatalf(string, ...interface{}) { panic("CONTROL_STORAGE_FATAL") }
