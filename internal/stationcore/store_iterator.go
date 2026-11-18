package stationcore

import (
	"bytes"
	"container/heap"

	"github.com/cockroachdb/pebble/v2"
)

// storeIterator merges the already sorted streams from active shards with one
// fixed-size min-heap. It retains at most one head per active shard and each
// step is O(log 64), without materializing deadline or counter rows.
type storeIterator struct {
	store   *store
	iters   []*pebble.Iterator
	current int
	valid   bool
	closed  bool
	heads   iteratorHeadHeap
}

func (iter *storeIterator) First() bool {
	iter.heads.owner = iter
	iter.heads.indexes = iter.heads.indexes[:0]
	for _, shardIter := range iter.iters {
		shardIter.First()
	}
	for index, shardIter := range iter.iters {
		if shardIter.Valid() {
			iter.heads.indexes = append(iter.heads.indexes, index)
		}
	}
	heap.Init(&iter.heads)
	return iter.selectMinimum()
}

func (iter *storeIterator) Next() bool {
	if !iter.valid {
		return false
	}
	advanced := heap.Pop(&iter.heads).(int)
	if iter.iters[advanced].Next() {
		heap.Push(&iter.heads, advanced)
	}
	return iter.selectMinimum()
}

func (iter *storeIterator) selectMinimum() bool {
	iter.valid = iter.heads.Len() > 0
	if iter.valid {
		iter.current = iter.heads.indexes[0]
	} else {
		iter.current = -1
	}
	return iter.valid
}

func (iter *storeIterator) Valid() bool { return iter.valid }

func (iter *storeIterator) Key() []byte {
	if !iter.valid {
		return nil
	}
	return iter.iters[iter.current].Key()
}

type iteratorHeadHeap struct {
	owner   *storeIterator
	indexes []int
}

func (heads iteratorHeadHeap) Len() int { return len(heads.indexes) }
func (heads iteratorHeadHeap) Less(left, right int) bool {
	return bytes.Compare(heads.owner.iters[heads.indexes[left]].Key(), heads.owner.iters[heads.indexes[right]].Key()) < 0
}
func (heads iteratorHeadHeap) Swap(left, right int) {
	heads.indexes[left], heads.indexes[right] = heads.indexes[right], heads.indexes[left]
}
func (heads *iteratorHeadHeap) Push(value any) { heads.indexes = append(heads.indexes, value.(int)) }
func (heads *iteratorHeadHeap) Pop() any {
	last := len(heads.indexes) - 1
	value := heads.indexes[last]
	heads.indexes = heads.indexes[:last]
	return value
}

func (iter *storeIterator) Value() []byte {
	if !iter.valid {
		return nil
	}
	return iter.iters[iter.current].Value()
}

func (iter *storeIterator) Error() error {
	for _, shardIter := range iter.iters {
		if err := shardIter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (iter *storeIterator) Close() error {
	if iter.closed {
		return nil
	}
	iter.closed = true
	var result error
	for _, shardIter := range iter.iters {
		if err := shardIter.Close(); err != nil && result == nil {
			result = err
		}
	}
	iter.store.mu.RUnlock()
	return result
}
