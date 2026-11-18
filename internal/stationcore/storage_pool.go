package stationcore

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
)

const (
	storagePoolCacheBytes       = 128 << 20
	storagePoolMaxOpenFiles     = 1024
	storagePoolFilesPerDatabase = storagePoolMaxOpenFiles / NamespacePartitions
	storagePoolCompactions      = 4
)

// StoragePool owns the bounded process-level resources shared by every shard
// DB. It prevents an InstanceSharded Core and a 64-partition Cluster Node from
// multiplying cache memory or durable-commit admission by the DB count.
type StoragePool struct {
	cache *pebble.Cache

	mu          sync.Mutex
	closed      bool
	stores      int
	commitCount int
	commitBytes int64
	compactions int
	schedulers  map[*poolCompactionScheduler]struct{}
	condition   *sync.Cond
	wake        chan struct{}
	stop        chan struct{}
	done        chan struct{}

	compactionScans atomic.Uint64
}

func NewStoragePool() *StoragePool {
	pool := &StoragePool{
		cache: pebble.NewCache(storagePoolCacheBytes), schedulers: make(map[*poolCompactionScheduler]struct{}),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	pool.condition = sync.NewCond(&pool.mu)
	go pool.compactionLoop()
	return pool
}

func (pool *StoragePool) attach() bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		return false
	}
	pool.stores++
	return true
}

func (pool *StoragePool) detach() {
	pool.mu.Lock()
	if pool.stores > 0 {
		pool.stores--
	}
	pool.mu.Unlock()
}

func (pool *StoragePool) reserveCommit(bytes int64) bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed || bytes <= 0 || bytes > groupCommitQueueBytes ||
		pool.commitCount >= groupCommitQueueEntries || pool.commitBytes > groupCommitQueueBytes-bytes {
		return false
	}
	pool.commitCount++
	pool.commitBytes += bytes
	return true
}

func (pool *StoragePool) releaseCommit(bytes int64) {
	pool.mu.Lock()
	pool.commitCount--
	pool.commitBytes -= bytes
	pool.mu.Unlock()
}

// Close releases the shared cache only after every attached store is closed.
// A premature close fails closed and leaves the pool usable by its owners.
func (pool *StoragePool) Close() error {
	pool.mu.Lock()
	if pool.stores != 0 || pool.commitCount != 0 || pool.compactions != 0 || len(pool.schedulers) != 0 {
		pool.mu.Unlock()
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}
	pool.closed = true
	close(pool.stop)
	pool.mu.Unlock()
	<-pool.done
	pool.cache.Unref()
	return nil
}

func (pool *StoragePool) options() *pebble.Options {
	opts := &pebble.Options{
		FormatMajorVersion: pebbleFormatMajor,
		Logger:             coreStorageLogger{},
		Cache:              pool.cache,
		MaxOpenFiles:       storagePoolFilesPerDatabase,
		MemTableSize:       1 << 20,
		CompactionConcurrencyRange: func() (int, int) {
			return 1, 1
		},
	}
	opts.Experimental.FileCacheShards = 1
	opts.Experimental.CompactionScheduler = &poolCompactionScheduler{pool: pool}
	opts.EnsureDefaults()
	return opts
}

type poolCompactionScheduler struct {
	pool       *StoragePool
	db         pebble.DBForCompaction
	registered bool
	running    int
	granting   bool
}

func (scheduler *poolCompactionScheduler) Register(_ int, db pebble.DBForCompaction) {
	scheduler.pool.mu.Lock()
	scheduler.db = db
	scheduler.registered = true
	scheduler.pool.schedulers[scheduler] = struct{}{}
	scheduler.pool.mu.Unlock()
}

func (scheduler *poolCompactionScheduler) Unregister() {
	pool := scheduler.pool
	pool.mu.Lock()
	scheduler.registered = false
	delete(pool.schedulers, scheduler)
	for scheduler.granting {
		pool.condition.Wait()
	}
	pool.mu.Unlock()
}

func (scheduler *poolCompactionScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	pool := scheduler.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !scheduler.registered || scheduler.running >= 1 || pool.compactions >= storagePoolCompactions {
		return false, nil
	}
	scheduler.running++
	pool.compactions++
	return true, &poolCompactionGrant{scheduler: scheduler}
}

func (scheduler *poolCompactionScheduler) UpdateGetAllowedWithoutPermission() {
	scheduler.pool.wakeCompactions()
}

type poolCompactionGrant struct {
	scheduler *poolCompactionScheduler
	done      atomic.Bool
}

func (*poolCompactionGrant) Started()                                          {}
func (*poolCompactionGrant) MeasureCPU(pebble.CompactionGoroutineKind)         {}
func (*poolCompactionGrant) CumulativeStats(pebble.CompactionGrantHandleStats) {}
func (grant *poolCompactionGrant) Done() {
	if !grant.done.CompareAndSwap(false, true) {
		return
	}
	pool := grant.scheduler.pool
	pool.mu.Lock()
	if grant.scheduler.running > 0 {
		grant.scheduler.running--
		pool.compactions--
	}
	pool.condition.Broadcast()
	pool.mu.Unlock()
	pool.wakeCompactions()
}

func (pool *StoragePool) wakeCompactions() {
	select {
	case pool.wake <- struct{}{}:
	default:
	}
}

func (pool *StoragePool) compactionLoop() {
	defer close(pool.done)
	for {
		select {
		case <-pool.wake:
			pool.grantWaitingCompactions()
		case <-pool.stop:
			return
		}
	}
}

func (pool *StoragePool) grantWaitingCompactions() {
	pool.compactionScans.Add(1)
	tried := make(map[*poolCompactionScheduler]struct{})
	for {
		pool.mu.Lock()
		if pool.compactions >= storagePoolCompactions {
			pool.mu.Unlock()
			return
		}
		var selected *poolCompactionScheduler
		for scheduler := range pool.schedulers {
			if _, already := tried[scheduler]; scheduler.registered && scheduler.running == 0 && !scheduler.granting && !already {
				selected = scheduler
				break
			}
		}
		if selected == nil {
			pool.mu.Unlock()
			return
		}
		selected.granting = true
		selected.running++
		pool.compactions++
		pool.mu.Unlock()

		waiting, _ := selected.db.GetWaitingCompaction()
		accepted := false
		grant := &poolCompactionGrant{scheduler: selected}
		if waiting {
			accepted = selected.db.Schedule(grant)
		}
		if !accepted {
			// The DB did not consume the grant, so return the reservation
			// without waking the scanner again. Calling Done here used to
			// create a self-sustaining wake loop across every idle DB.
			pool.mu.Lock()
			selected.running--
			pool.compactions--
			pool.mu.Unlock()
		}

		pool.mu.Lock()
		selected.granting = false
		pool.condition.Broadcast()
		pool.mu.Unlock()
		tried[selected] = struct{}{}
	}
}
