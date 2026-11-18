//go:build station_acceptance

package acceptance

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	latencyHistogramShards     = 16
	latencyHistogramSubBuckets = 32
	latencyHistogramBuckets    = 63 * latencyHistogramSubBuckets
)

type latencyHistogramShard struct {
	buckets [latencyHistogramBuckets]atomic.Uint64
}

type loadMetrics struct {
	mu sync.Mutex

	latencyShards  [latencyHistogramShards]latencyHistogramShard
	latencyCursor  atomic.Uint64
	namespaceIndex map[string]int
	nsCompletions  [maxCapacityNamespaces]atomic.Uint64
	nsAccepted     [maxCapacityNamespaces]atomic.Uint64
	peakRSS        uint64
	logicalBytes   uint64
	diskBytes      uint64
	completedUnits uint64
	claimedUnits   uint64
	claimCalls     uint64
	maxClaimBatch  uint64
	segmentStart   time.Time
	segmentEnd     time.Time
	hourlyRSS      []uint64
	lastHourSample time.Time
}

func newLoadMetrics() *loadMetrics {
	metrics := &loadMetrics{namespaceIndex: make(map[string]int, maxCapacityNamespaces)}
	for index := 0; index < maxCapacityNamespaces; index++ {
		metrics.namespaceIndex[namespaceID(index)] = index
	}
	return metrics
}

func (metrics *loadMetrics) beginSegment() {
	metrics.mu.Lock()
	metrics.segmentStart = time.Now()
	metrics.segmentEnd = time.Time{}
	metrics.lastHourSample = metrics.segmentStart
	metrics.mu.Unlock()
	atomic.StoreUint64(&metrics.completedUnits, 0)
	atomic.StoreUint64(&metrics.claimedUnits, 0)
	atomic.StoreUint64(&metrics.claimCalls, 0)
	atomic.StoreUint64(&metrics.maxClaimBatch, 0)
	for index := 0; index < maxCapacityNamespaces; index++ {
		metrics.nsCompletions[index].Store(0)
		metrics.nsAccepted[index].Store(0)
	}
	metrics.latencyCursor.Store(0)
	for shard := range metrics.latencyShards {
		for bucket := range metrics.latencyShards[shard].buckets {
			metrics.latencyShards[shard].buckets[bucket].Store(0)
		}
	}
}

func (metrics *loadMetrics) endSegment() {
	metrics.mu.Lock()
	metrics.segmentEnd = time.Now()
	metrics.mu.Unlock()
}

func (metrics *loadMetrics) recordLatency(d time.Duration) {
	sequence := metrics.latencyCursor.Add(1) - 1
	shard := sequence % latencyHistogramShards
	bucket := latencyBucket(d)
	metrics.latencyShards[shard].buckets[bucket].Add(1)
}

func (metrics *loadMetrics) recordCompletion(namespace string, count uint64) {
	atomic.AddUint64(&metrics.completedUnits, count)
	if index, ok := metrics.namespaceIndex[namespace]; ok {
		metrics.nsCompletions[index].Add(count)
	}
}

func (metrics *loadMetrics) recordAccepted(namespace string) {
	if index, ok := metrics.namespaceIndex[namespace]; ok {
		metrics.nsAccepted[index].Add(1)
	}
}

func (metrics *loadMetrics) recordClaimBatch(count int) {
	if count <= 0 {
		return
	}
	atomic.AddUint64(&metrics.claimedUnits, uint64(count))
	atomic.AddUint64(&metrics.claimCalls, 1)
	for {
		previous := atomic.LoadUint64(&metrics.maxClaimBatch)
		if uint64(count) <= previous || atomic.CompareAndSwapUint64(&metrics.maxClaimBatch, previous, uint64(count)) {
			return
		}
	}
}

func (metrics *loadMetrics) sampleRSS() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss := stats.Sys
	for {
		prev := atomic.LoadUint64(&metrics.peakRSS)
		if rss <= prev || atomic.CompareAndSwapUint64(&metrics.peakRSS, prev, rss) {
			break
		}
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.lastHourSample.IsZero() {
		metrics.lastHourSample = time.Now()
		return
	}
	if time.Since(metrics.lastHourSample) >= time.Hour {
		metrics.hourlyRSS = append(metrics.hourlyRSS, rss)
		metrics.lastHourSample = time.Now()
	}
}

func (metrics *loadMetrics) setStorage(logical, disk uint64) {
	atomic.StoreUint64(&metrics.logicalBytes, logical)
	atomic.StoreUint64(&metrics.diskBytes, disk)
}

func (metrics *loadMetrics) result(namespaceCount int) CapacityResult {
	metrics.mu.Lock()
	segmentStart := metrics.segmentStart
	segmentEnd := metrics.segmentEnd
	metrics.mu.Unlock()
	completions := make(map[string]uint64, namespaceCount)
	accepted := make(map[string]uint64, namespaceCount)
	for index := 0; index < namespaceCount; index++ {
		namespace := namespaceID(index)
		completions[namespace] = metrics.nsCompletions[index].Load()
		accepted[namespace] = metrics.nsAccepted[index].Load()
	}

	if segmentEnd.IsZero() {
		segmentEnd = time.Now()
	}
	elapsedDuration := segmentEnd.Sub(segmentStart)
	elapsed := elapsedDuration.Seconds()
	if elapsed <= 0 {
		elapsedDuration = time.Second
		elapsed = elapsedDuration.Seconds()
	}

	completed := atomic.LoadUint64(&metrics.completedUnits)
	logical := atomic.LoadUint64(&metrics.logicalBytes)
	disk := atomic.LoadUint64(&metrics.diskBytes)
	claimed := atomic.LoadUint64(&metrics.claimedUnits)
	claimCalls := atomic.LoadUint64(&metrics.claimCalls)

	amplification := 0.0
	if logical > 0 {
		amplification = float64(disk) / float64(logical)
	}

	result := CapacityResult{
		Elapsed:                elapsedDuration,
		EndToEndUnitsPerSecond: float64(completed) / elapsed,
		MutationLatencyP95:     metrics.latencyPercentile(95, 100),
		MutationLatencyP99:     metrics.latencyPercentile(99, 100),
		JainFairness:           jainServiceFairness(completions, accepted, namespaceCount),
		RSSBytes:               atomic.LoadUint64(&metrics.peakRSS),
		StorageAmplification:   amplification,
		MaxClaimBatchSize:      atomic.LoadUint64(&metrics.maxClaimBatch),
	}
	if claimCalls > 0 {
		result.MeanClaimBatchSize = float64(claimed) / float64(claimCalls)
	}
	return result
}

func latencyBucket(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}
	value := uint64(duration)
	exponent := bits.Len64(value) - 1
	base := uint64(1) << exponent
	var subBucket int
	if exponent < 5 {
		subBucket = int(((value - base) * latencyHistogramSubBuckets) / base)
	} else {
		subBucket = int((value - base) / (base / latencyHistogramSubBuckets))
	}
	if subBucket >= latencyHistogramSubBuckets {
		subBucket = latencyHistogramSubBuckets - 1
	}
	return exponent*latencyHistogramSubBuckets + subBucket
}

func latencyBucketUpper(index int) time.Duration {
	if index <= 0 {
		return time.Nanosecond
	}
	exponent := index / latencyHistogramSubBuckets
	subBucket := index % latencyHistogramSubBuckets
	base := uint64(1) << exponent
	var increment uint64
	if exponent < 5 {
		increment = (uint64(subBucket+1)*base + latencyHistogramSubBuckets - 1) / latencyHistogramSubBuckets
	} else {
		increment = uint64(subBucket+1) * (base / latencyHistogramSubBuckets)
	}
	upper := base + increment - 1
	if upper > uint64(^uint64(0)>>1) {
		return time.Duration(^uint64(0) >> 1)
	}
	return time.Duration(upper)
}

func (metrics *loadMetrics) latencyPercentile(numerator, denominator uint64) time.Duration {
	var totals [latencyHistogramBuckets]uint64
	var count uint64
	for shard := range metrics.latencyShards {
		for bucket := range metrics.latencyShards[shard].buckets {
			value := metrics.latencyShards[shard].buckets[bucket].Load()
			totals[bucket] += value
			count += value
		}
	}
	if count == 0 || denominator == 0 {
		return 0
	}
	target := (count*numerator + denominator - 1) / denominator
	var cumulative uint64
	for bucket, value := range totals {
		cumulative += value
		if cumulative >= target {
			return latencyBucketUpper(bucket)
		}
	}
	return latencyBucketUpper(latencyHistogramBuckets - 1)
}

func jainServiceFairness(completions, accepted map[string]uint64, namespaceCount int) float64 {
	if namespaceCount <= 0 {
		return 1
	}
	ratios := make([]float64, 0, namespaceCount)
	for index := 0; index < namespaceCount; index++ {
		namespace := namespaceID(index)
		if accepted[namespace] == 0 {
			continue
		}
		ratios = append(ratios, float64(completions[namespace])/float64(accepted[namespace]))
	}
	if len(ratios) == 0 {
		return 1
	}
	sum := 0.0
	sumSq := 0.0
	for _, ratio := range ratios {
		sum += ratio
		sumSq += ratio * ratio
	}
	if sumSq == 0 {
		return 1
	}
	n := float64(len(ratios))
	return (sum * sum) / (n * sumSq)
}

func (metrics *loadMetrics) memoryGrowthBetweenHours() float64 {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if len(metrics.hourlyRSS) < 2 {
		return 0
	}
	first := float64(metrics.hourlyRSS[0])
	last := float64(metrics.hourlyRSS[len(metrics.hourlyRSS)-1])
	if first == 0 {
		return 0
	}
	return (last - first) / first
}
