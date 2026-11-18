package stationcore

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	groupCommitQueueEntries = 256
	groupCommitQueueBytes   = 64 << 20
	groupCommitMaxEntries   = 128
	groupCommitMaxBytes     = 16 << 20
	groupCommitMaxWait      = 250 * time.Microsecond
	groupCommitRetryDelay   = time.Millisecond
)

type commitOutcome struct {
	ticket uint64
	err    error
}

type groupCommitRequest struct {
	batch  *pebble.Batch
	bytes  int64
	result chan commitOutcome
}

// durabilityTicket identifies the barrier sync that must complete before one
// mutation response may be acknowledged. A zero ticket needs no confirmation
// because the mutation performed no durable state transition.
type durabilityTicket struct {
	committer *groupCommitter
	ticket    uint64
}

func (ticket durabilityTicket) await() error {
	if ticket.committer == nil || ticket.ticket == 0 {
		return nil
	}
	return ticket.committer.await(ticket.ticket)
}

// groupCommitter owns the durable commit path of one Pebble database.
//
// A logical batch is applied to the memtable and appended to the write-ahead
// log as soon as its group is combined (pebble.NoSync), so every later read on
// the same database observes it and the mutation lanes stay serialized by
// commit order. Durability is confirmed independently: the barrier goroutine
// issues a covering synchronous write-ahead-log record, and every caller waits
// for the barrier that covers its own ticket. A response is therefore never
// returned before its commit is durable, while a caller that only needs the
// state transition (a mutation lane) never waits for the fsync itself. One
// station namespace therefore keeps committing at memory speed instead of
// being bounded by the storage fsync latency.
type groupCommitter struct {
	db   *pebble.DB
	pool *StoragePool

	queue chan *groupCommitRequest
	stop  chan struct{}
	done  chan struct{}
	wake  chan struct{}

	barrierDone chan struct{}

	closeOnce   sync.Once
	queuedBytes atomic.Int64

	// submitted counts logical batches in commit order; durable is the highest
	// ticket covered by a completed barrier sync. A barrier only ever covers
	// batches that were already appended to the log when it was issued, so a
	// ticket is confirmed exactly once its log record is covered by an fsync.
	submitted atomic.Uint64
	durable   atomic.Uint64

	// waitMu guards the barrier result: failedUpTo is the highest ticket that
	// belongs to a failed barrier (its callers must not treat the mutation as
	// durable), syncError is the first barrier failure, and closed marks a
	// stopped committer so no waiter can block forever.
	waitMu     sync.Mutex
	waitCond   *sync.Cond
	failedUpTo uint64
	syncError  error
	closed     bool

	physicalSyncs atomic.Uint64
	maxGroupSize  atomic.Uint64
	logicalBytes  atomic.Uint64
	physicalBytes atomic.Uint64
}

func newGroupCommitter(db *pebble.DB, pool *StoragePool) *groupCommitter {
	committer := &groupCommitter{
		db:          db,
		pool:        pool,
		queue:       make(chan *groupCommitRequest, groupCommitQueueEntries),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		wake:        make(chan struct{}, 1),
		barrierDone: make(chan struct{}),
	}
	committer.waitCond = sync.NewCond(&committer.waitMu)
	go committer.loop()
	go committer.barrierLoop()
	return committer
}

// submit combines one logical batch into the next committed group and returns
// its durability ticket. The batch is visible to later reads when submit
// returns; it is durable once the returned ticket has been confirmed.
func (committer *groupCommitter) submit(batch *pebble.Batch) (uint64, error) {
	bytes := int64(batch.Len())
	if bytes <= 0 || bytes > groupCommitMaxBytes || !committer.pool.reserveCommit(bytes) {
		_ = batch.Close()
		return 0, pebble.ErrClosed
	}
	if !committer.reserveBytes(bytes) {
		committer.pool.releaseCommit(bytes)
		_ = batch.Close()
		return 0, pebble.ErrClosed
	}
	request := &groupCommitRequest{
		batch:  batch,
		bytes:  bytes,
		result: make(chan commitOutcome, 1),
	}
	select {
	case committer.queue <- request:
	case <-committer.stop:
		committer.release(request)
		return 0, pebble.ErrClosed
	default:
		committer.release(request)
		return 0, pebble.ErrClosed
	}
	outcome := <-request.result
	return outcome.ticket, outcome.err
}

func (committer *groupCommitter) reserveBytes(bytes int64) bool {
	for {
		current := committer.queuedBytes.Load()
		if current > groupCommitQueueBytes-bytes {
			return false
		}
		if committer.queuedBytes.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

func (committer *groupCommitter) loop() {
	defer close(committer.done)
	var carry *groupCommitRequest
	for {
		first := carry
		carry = nil
		if first == nil {
			select {
			case first = <-committer.queue:
			case <-committer.stop:
				committer.failQueued()
				return
			}
		}

		group := make([]*groupCommitRequest, 0, groupCommitMaxEntries)
		group = append(group, first)
		groupBytes := first.bytes
		timer := time.NewTimer(groupCommitMaxWait)
	collect:
		for len(group) < groupCommitMaxEntries {
			select {
			case request := <-committer.queue:
				if groupBytes+request.bytes > groupCommitMaxBytes {
					carry = request
					break collect
				}
				group = append(group, request)
				groupBytes += request.bytes
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		committer.commit(group)
		select {
		case <-committer.stop:
			committer.failQueued()
			return
		default:
		}
	}
}

func (committer *groupCommitter) commit(group []*groupCommitRequest) {
	combined := committer.db.NewBatch()
	var commitErr error
	for _, request := range group {
		if commitErr == nil {
			commitErr = combined.Apply(request.batch, nil)
		}
	}
	var ticket uint64
	if commitErr == nil {
		physicalBytes := uint64(combined.Len())
		// Apply the group without waiting for the write-ahead-log fsync. The
		// group becomes durable when the barrier that covers its ticket
		// completes; its callers wait for that barrier, not for this call.
		commitErr = combined.Commit(pebble.NoSync)
		if commitErr == nil {
			committer.physicalBytes.Add(physicalBytes)
			updateAtomicMaximum(&committer.maxGroupSize, uint64(len(group)))
			for _, request := range group {
				committer.logicalBytes.Add(uint64(request.bytes))
			}
			ticket = committer.submitted.Add(1)
			committer.wakeBarrier()
		}
	}
	_ = combined.Close()
	for _, request := range group {
		committer.release(request)
		request.result <- commitOutcome{ticket: ticket, err: commitErr}
	}
}

// barrierLoop issues one covering synchronous write-ahead-log record per
// outstanding ticket window. A record appended after every earlier commit is
// flushed only after those commits reached the log, so one fsync confirms
// every ticket at or below the coverage it observed.
func (committer *groupCommitter) barrierLoop() {
	defer close(committer.barrierDone)
	for {
		select {
		case <-committer.stop:
			return
		case <-committer.wake:
		}
		for {
			committer.waitMu.Lock()
			covered := committer.submitted.Load()
			pending := covered > committer.durable.Load()
			committer.waitMu.Unlock()
			if !pending {
				break
			}
			err := committer.db.LogData(nil, pebble.Sync)
			committer.physicalSyncs.Add(1)
			committer.waitMu.Lock()
			if err == nil {
				committer.durable.Store(covered)
			} else {
				if committer.failedUpTo < covered {
					committer.failedUpTo = covered
				}
				if committer.syncError == nil {
					committer.syncError = err
				}
			}
			committer.waitCond.Broadcast()
			committer.waitMu.Unlock()
			if err == nil {
				continue
			}
			// A failed barrier leaves its commit window unconfirmed and gets
			// retried after a bounded delay, so a transient storage error does
			// not spin and a permanent one cannot block shutdown.
			select {
			case <-committer.stop:
				return
			case <-time.After(groupCommitRetryDelay):
			}
		}
	}
}

func (committer *groupCommitter) wakeBarrier() {
	select {
	case committer.wake <- struct{}{}:
	default:
	}
}

// await blocks until the barrier that covers the given ticket completed.
func (committer *groupCommitter) await(ticket uint64) error {
	committer.waitMu.Lock()
	defer committer.waitMu.Unlock()
	for {
		if committer.durable.Load() >= ticket {
			return nil
		}
		if committer.failedUpTo >= ticket {
			if committer.syncError != nil {
				return committer.syncError
			}
			return pebble.ErrClosed
		}
		if committer.closed {
			return pebble.ErrClosed
		}
		committer.waitCond.Wait()
	}
}

// drain confirms durability for every batch submitted so far. Callers hold the
// store lifecycle write lock, so no new batch can be admitted while it runs,
// and the loop is defensive against a late submission.
func (committer *groupCommitter) drain() error {
	for {
		ticket := committer.submitted.Load()
		if ticket == 0 || committer.durable.Load() >= ticket {
			return nil
		}
		if err := committer.await(ticket); err != nil {
			return err
		}
		if committer.submitted.Load() == ticket {
			return nil
		}
	}
}

func (committer *groupCommitter) release(request *groupCommitRequest) {
	committer.queuedBytes.Add(-request.bytes)
	committer.pool.releaseCommit(request.bytes)
	_ = request.batch.Close()
}

// failQueued answers every request that was admitted but never committed. The
// store only stops a committer after the lifecycle write lock proved that no
// mutation is in flight, so this path only protects a failed startup.
func (committer *groupCommitter) failQueued() {
	for {
		select {
		case request := <-committer.queue:
			committer.release(request)
			request.result <- commitOutcome{err: pebble.ErrClosed}
		default:
			return
		}
	}
}

func (committer *groupCommitter) close() {
	committer.closeOnce.Do(func() {
		close(committer.stop)
		<-committer.done
		<-committer.barrierDone
		committer.waitMu.Lock()
		committer.closed = true
		if committer.failedUpTo < committer.submitted.Load() {
			committer.failedUpTo = committer.submitted.Load()
		}
		committer.waitCond.Broadcast()
		committer.waitMu.Unlock()
	})
}

func updateAtomicMaximum(value *atomic.Uint64, candidate uint64) {
	for {
		current := value.Load()
		if current >= candidate || value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
