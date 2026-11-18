package stationcore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type mutationKind int

const (
	mutationReserve mutationKind = iota + 1
	mutationSubmit
	mutationClaim
	mutationSettle
)

type mutationJob struct {
	kind       mutationKind
	namespace  string
	priority   bool
	charge     int64
	queueBytes int64
	run        func() error
	done       chan error

	// durability is captured by the owning lane after the job ran. The caller
	// awaits it before it returns a response, so a mutation is acknowledged
	// only after the barrier that covers its commit completed, while the lane
	// itself never waits for the fsync.
	durability durabilityTicket
}

type claimWaiter struct {
	addressKey string
	ch         chan struct{}
}

type mutationLane struct {
	id            int
	mu            sync.Mutex
	cond          *sync.Cond
	queue         []*mutationJob
	normalQueued  int
	controlQueued int
	deficit       map[string]int64
	ring          []string
	ringIdx       int
	waiters       map[string][]*claimWaiter
	closed        bool
	done          chan struct{}
	core          *Core
	sched         *scheduler
}

type scheduler struct {
	lanes       []*mutationLane
	queuedBytes atomic.Int64
}

func newScheduler(core *Core) *scheduler {
	laneCount := mutationLanes
	if core.config.StorageMode == StorageModePartitionOne {
		laneCount = partitionMutationLanes
	}
	sched := &scheduler{lanes: make([]*mutationLane, 0, laneCount)}
	for laneID := 0; laneID < laneCount; laneID++ {
		lane := &mutationLane{
			id:      laneID,
			deficit: make(map[string]int64),
			waiters: make(map[string][]*claimWaiter),
			done:    make(chan struct{}),
			core:    core,
			sched:   sched,
		}
		lane.cond = sync.NewCond(&lane.mu)
		sched.lanes = append(sched.lanes, lane)
		go lane.loop()
	}
	return sched
}

func (sched *scheduler) laneFor(namespace string) *mutationLane {
	return sched.lanes[namespaceLane(namespace)%len(sched.lanes)]
}

func (sched *scheduler) run(ctx context.Context, namespace string, priority bool, charge int64, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	job := &mutationJob{
		kind:      mutationSubmit,
		namespace: namespace,
		priority:  priority,
		charge:    charge,
		run:       fn,
		done:      make(chan error, 1),
	}
	job.queueBytes = charge
	if job.queueBytes < drrChargeGranularity {
		job.queueBytes = drrChargeGranularity
	}
	if !sched.reserveQueueBytes(priority, job.queueBytes) {
		return fail(ReasonRateLimited, true, 100)
	}
	lane := sched.laneFor(namespace)
	lane.mu.Lock()
	if lane.closed {
		lane.mu.Unlock()
		sched.releaseQueueBytes(job.queueBytes)
		return fail(ReasonDraining, true, 100)
	}
	if (!priority && lane.normalQueued >= maxMutationQueueEntriesPerNS) ||
		(priority && lane.controlQueued >= maxControlQueueEntriesPerLane) {
		lane.mu.Unlock()
		sched.releaseQueueBytes(job.queueBytes)
		return fail(ReasonRateLimited, true, 100)
	}
	lane.enqueueLocked(job)
	lane.mu.Unlock()
	select {
	case err := <-job.done:
		if err != nil {
			return err
		}
		// The lane applied the mutation and captured the durability ticket
		// that covers it. Only this caller waits for the barrier, so the lane
		// is already serving the next mutation of its namespace.
		return job.durability.await()
	case <-ctx.Done():
		lane.cancelJob(job)
		return ctx.Err()
	}
}

func (sched *scheduler) reserveQueueBytes(priority bool, charge int64) bool {
	limit := int64(maxMutationQueueBytes)
	if priority {
		limit += reservedControlQueueBytes
	}
	for {
		current := sched.queuedBytes.Load()
		if charge > limit || current > limit-charge {
			return false
		}
		if sched.queuedBytes.CompareAndSwap(current, current+charge) {
			return true
		}
	}
}

func (sched *scheduler) releaseQueueBytes(charge int64) {
	sched.queuedBytes.Add(-charge)
}

func (lane *mutationLane) enqueueLocked(job *mutationJob) {
	lane.queue = append(lane.queue, job)
	if job.priority {
		lane.controlQueued++
	} else {
		lane.normalQueued++
	}
	lane.touchNamespaceLocked(job.namespace)
	lane.cond.Signal()
}

func (lane *mutationLane) touchNamespaceLocked(namespace string) {
	for _, ns := range lane.ring {
		if ns == namespace {
			return
		}
	}
	lane.ring = append(lane.ring, namespace)
}

func (lane *mutationLane) loop() {
	defer close(lane.done)
	for {
		lane.mu.Lock()
		for len(lane.queue) == 0 && !lane.closed {
			lane.cond.Wait()
		}
		if len(lane.queue) == 0 && lane.closed {
			lane.mu.Unlock()
			return
		}
		job := lane.pickJobLocked()
		lane.mu.Unlock()
		if job == nil {
			continue
		}
		err := job.run()
		if err == nil {
			// Capture the ticket that covers this job's commit (and every
			// earlier commit of its namespace) before the lane moves on. The
			// caller awaits it; the lane never waits for the fsync.
			job.durability = lane.core.store.durabilityForNamespace(job.namespace)
		}
		job.done <- err
	}
}

func (lane *mutationLane) pickJobLocked() *mutationJob {
	if len(lane.queue) == 0 {
		return nil
	}
	for attempts := 0; attempts < len(lane.queue); attempts++ {
		idx := lane.selectIndexLocked()
		if idx < 0 {
			idx = 0
		}
		job := lane.queue[idx]
		ns := job.namespace
		def := lane.deficit[ns]
		if job.priority || def+job.charge <= drrQuantumBytes {
			lane.queue = append(lane.queue[:idx], lane.queue[idx+1:]...)
			lane.decrementQueuedLocked(job)
			if job.priority {
				lane.deficit[ns] = def
			} else {
				lane.deficit[ns] = def + job.charge - drrQuantumBytes
			}
			if len(lane.ring) > 0 {
				lane.ringIdx = (lane.ringIdx + 1) % len(lane.ring)
			}
			return job
		}
		if len(lane.ring) > 0 {
			lane.ringIdx = (lane.ringIdx + 1) % len(lane.ring)
			lane.deficit[lane.ring[lane.ringIdx]] += drrQuantumBytes
		}
	}
	job := lane.queue[0]
	lane.queue = lane.queue[1:]
	lane.decrementQueuedLocked(job)
	return job
}

func (lane *mutationLane) decrementQueuedLocked(job *mutationJob) {
	if job.priority {
		lane.controlQueued--
	} else {
		lane.normalQueued--
	}
	lane.sched.releaseQueueBytes(job.queueBytes)
}

func (lane *mutationLane) selectIndexLocked() int {
	if len(lane.ring) == 0 {
		return 0
	}
	target := lane.ring[lane.ringIdx]
	for i, job := range lane.queue {
		if job.namespace == target {
			return i
		}
	}
	for i, job := range lane.queue {
		if job.priority {
			return i
		}
	}
	return 0
}

func (lane *mutationLane) cancelJob(job *mutationJob) {
	lane.mu.Lock()
	defer lane.mu.Unlock()
	for i, queued := range lane.queue {
		if queued == job {
			lane.queue = append(lane.queue[:i], lane.queue[i+1:]...)
			lane.decrementQueuedLocked(job)
			job.done <- context.Canceled
			return
		}
	}
}

func (sched *scheduler) close() {
	for _, lane := range sched.lanes {
		lane.mu.Lock()
		lane.closed = true
		lane.cond.Broadcast()
		lane.mu.Unlock()
	}
	for _, lane := range sched.lanes {
		<-lane.done
	}
}

func (lane *mutationLane) registerWaiter(addressKey string) (*claimWaiter, error) {
	lane.mu.Lock()
	defer lane.mu.Unlock()
	if len(lane.waiters[addressKey]) >= maxLongPollClaimsPerNS {
		return nil, fail(ReasonRateLimited, true, 100)
	}
	waiter := &claimWaiter{addressKey: addressKey, ch: make(chan struct{}, 1)}
	lane.waiters[addressKey] = append(lane.waiters[addressKey], waiter)
	return waiter, nil
}

func (lane *mutationLane) removeWaiter(addressKey string, waiter *claimWaiter) {
	lane.mu.Lock()
	defer lane.mu.Unlock()
	waiters := lane.waiters[addressKey]
	for i, w := range waiters {
		if w == waiter {
			lane.waiters[addressKey] = append(waiters[:i], waiters[i+1:]...)
			return
		}
	}
}

func (lane *mutationLane) wakeWaiters(addressKey string) {
	lane.mu.Lock()
	waiters := lane.waiters[addressKey]
	lane.waiters[addressKey] = nil
	lane.mu.Unlock()
	for _, waiter := range waiters {
		select {
		case waiter.ch <- struct{}{}:
		default:
		}
	}
}

func waitForClaim(ctx context.Context, waiter *claimWaiter, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter.ch:
		return nil
	case <-timer.C:
		return nil
	}
}
