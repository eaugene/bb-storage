package concurrencytest_test

// Microbenchmarks for the FindMissing() refresh path.
//
// These exist to make the central performance claim of the refresh
// striping change FALSIFIABLE. The claim is:
//
//	baseline  a single global refreshLock is held across the block
//	          copy, so wall time ~ finders x batch x copy_time
//	fixed     the lock is striped 256 ways by digest, so refreshes of
//	          distinct digests overlap and wall time ~ batch x copy_time
//
// The shaped prediction that follows is what to check: BASELINE
// throughput is roughly FLAT in finder count -- adding finders adds no
// parallelism, they just queue -- while FIXED throughput SCALES with
// finder count until the stripes saturate. If baseline throughput
// scales with finders, the diagnosis behind this change is wrong.
//
// Two design choices are load-bearing:
//
//  1. Every finder gets a DISJOINT digest set. Same-digest dedup is
//     already covered by TestFindMissingRefreshDedup; what is measured
//     here is cross-digest parallelism, which is the property striping
//     actually adds.
//
//  2. The modelled copy cost is a real sleep INSIDE the put writer,
//     i.e. inside the section the lock is held across. Sleeping parks
//     the goroutine, so when the lock is not held other finders make
//     progress and when it is held they do not -- exactly the property
//     under test. With an instantaneous stub the finders never overlap
//     and the benchmark measures nothing. That is not hypothetical:
//     TestFindMissingRefreshDedup originally passed with the stripe
//     lock removed for precisely this reason.
//
// NOTE: this file no longer compiles against the baseline tree --
// NewFlatBlobAccess gained a refreshConcurrency parameter. The A/B is
// now done in-tree instead, which is cleaner anyway: run with
// refreshConcurrency 1 to get the old single-threaded refresh, and with
// the default to get the striped one, against one binary. See
// refresh_stress_test.go for the real-stack counterpart.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// benchCopyDelay models the cost of copying one blob out of an old
// block and into a new one. Production reads on the affected cluster
// were ~8.5ms; 100us keeps the benchmark fast while staying far above
// scheduler noise, and the ratio between the trees is what matters.
const benchCopyDelay = 100 * time.Microsecond

const benchBlobSizeBytes = 256

func benchDigest(seq uint64, sizeBytes int64) digest.Digest {
	// Distinct, well-spread hashes without pulling in sha256: the
	// Key that FlatBlobAccess stripes on is a SHA-256 of the digest's
	// string form, so the stripe distribution does not depend on this.
	h := make([]byte, 32)
	x := seq*0x9E3779B97F4A7C15 + 0xBF58476D1CE4E5B9
	for i := range h {
		x ^= x >> 30
		x *= 0xBF58476D1CE4E5B9
		x ^= x >> 27
		h[i] = byte(x)
	}
	return digest.MustNewDigest("bench", remoteexecution.DigestFunction_SHA256,
		fmt.Sprintf("%x", h), sizeBytes)
}

// benchKeyLocationMap is a trivially correct KeyLocationMap. It is
// internally synchronised with a plain mutex so that these benchmarks
// measure FlatBlobAccess's refresh serialisation rather than the
// Robin Hood probe behaviour of hashingKeyLocationMap.
type benchKeyLocationMap struct {
	lock sync.Mutex
	m    map[local.Key]local.Location
}

func (klm *benchKeyLocationMap) Get(key local.Key) (local.Location, error) {
	klm.lock.Lock()
	defer klm.lock.Unlock()
	if location, ok := klm.m[key]; ok {
		return location, nil
	}
	return local.Location{}, status.Error(codes.NotFound, "Blob not found")
}

func (klm *benchKeyLocationMap) Put(key local.Key, location local.Location) error {
	klm.lock.Lock()
	defer klm.lock.Unlock()
	klm.m[key] = location
	return nil
}

// benchLocationBlobMap reports needsRefresh for anything in block 0,
// and its refresh deliberately writes the blob back into block 0 rather
// than migrating it. Blobs seeded there are therefore refreshed on
// every pass, which keeps the work per iteration constant and identical
// on both trees and removes the eviction confound that makes a
// ring-buffer harness stop exercising refresh after its first burst.
//
// A blob seeded in block 1 is never refreshed. That is what lets
// BenchmarkGetDuringRefresh measure read latency caused by OTHER
// goroutines' refresh traffic, rather than by the read's own refresh --
// which is the whole point of that benchmark and easy to get wrong.
type benchLocationBlobMap struct {
	copyDelay time.Duration

	allocations atomic.Int64
	lock        sync.Mutex
	nextOffset  int64
}

func (lbm *benchLocationBlobMap) Get(location local.Location) (local.LocationBlobGetter, bool) {
	return func(digest.Digest) buffer.Buffer {
		return buffer.NewValidatedBufferFromByteSlice(make([]byte, location.SizeBytes))
	}, location.BlockIndex == 0
}

func (lbm *benchLocationBlobMap) Put(sizeBytes int64) (local.LocationBlobPutWriter, error) {
	lbm.allocations.Add(1)

	lbm.lock.Lock()
	offsetBytes := lbm.nextOffset
	lbm.nextOffset += sizeBytes
	lbm.lock.Unlock()

	return func(b buffer.Buffer) local.LocationBlobPutFinalizer {
		b.Discard()
		// The copy. FlatBlobAccess runs this with the outer lock
		// released but -- in the baseline -- with the single global
		// refreshLock still held.
		time.Sleep(lbm.copyDelay)
		return func() (local.Location, error) {
			// Stays in block 0: the blob remains "old", so the next
			// pass refreshes it again and refresh pressure is steady.
			return local.Location{
				BlockIndex:  0,
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			}, nil
		}
	}, nil
}

// benchHarness wires a FlatBlobAccess over the two stubs above and
// hands back one disjoint digest set per finder.
func benchHarness(finders, batch int, copyDelay time.Duration) (blobstore.BlobAccess, []digest.Set, digest.Digest, *benchLocationBlobMap) {
	klm := &benchKeyLocationMap{m: map[local.Key]local.Location{}}
	lbm := &benchLocationBlobMap{copyDelay: copyDelay}

	sets := make([]digest.Set, finders)
	for f := 0; f < finders; f++ {
		sb := digest.NewSetBuilder(batch)
		for i := 0; i < batch; i++ {
			d := benchDigest(uint64(f)*1_000_000+uint64(i), benchBlobSizeBytes)
			sb.Add(d)
			klm.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{
				BlockIndex:  0,
				OffsetBytes: int64(i) * benchBlobSizeBytes,
				SizeBytes:   benchBlobSizeBytes,
			}
		}
		sets[f] = sb.Build()
	}

	// A blob in block 1: present, never in need of refresh. Reads of
	// this are what BenchmarkGetDuringRefresh times.
	fresh := benchDigest(999_000_000, benchBlobSizeBytes)
	klm.m[local.NewKeyFromString(fresh.GetKey(digest.KeyWithoutInstance))] = local.Location{
		BlockIndex:  1,
		OffsetBytes: 0,
		SizeBytes:   benchBlobSizeBytes,
	}

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		klm, lbm, digest.KeyWithoutInstance, &globalLock, 0, "bench",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)
	return access, sets, fresh, lbm
}

func benchmarkFindMissingRefresh(b *testing.B, finders, batch int, copyDelay time.Duration) {
	access, sets, _, lbm := benchHarness(finders, batch, copyDelay)
	ctx := context.Background()

	// Exactly b.N FindMissing calls in total, pulled off a shared
	// counter by `finders` goroutines, so the framework's ns/op stays
	// meaningful and the concurrency level is exactly what we asked for.
	var remaining atomic.Int64
	remaining.Store(int64(b.N))

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	var failed atomic.Bool
	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				if _, err := access.FindMissing(ctx, sets[id]); err != nil {
					failed.Store(true)
					return
				}
			}
		}(f)
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	if failed.Load() {
		b.Fatal("FindMissing returned an error")
	}
	if lbm.allocations.Load() == 0 {
		b.Fatal("no refreshes performed: the benchmark is vacuous")
	}

	// ns/op above is wall/b.N, i.e. inverse throughput. These make the
	// two things being compared explicit.
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "FindMissing/s")
	b.ReportMetric(float64(lbm.allocations.Load())/elapsed.Seconds(), "refresh/s")
	b.ReportMetric(elapsed.Seconds()*1000*float64(finders)/float64(b.N), "ms/call")
}

// BenchmarkFindMissingRefresh is THE falsifier. Sweep finder count and
// compare the throughput column between trees.
//
//	baseline  FindMissing/s roughly flat across finders
//	fixed     FindMissing/s scales with finders
func BenchmarkFindMissingRefresh(b *testing.B) {
	for _, finders := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("finders=%d", finders), func(b *testing.B) {
			benchmarkFindMissingRefresh(b, finders, 4, benchCopyDelay)
		})
	}
}

// BenchmarkFindMissingRefreshBatch sweeps the per-call batch size at a
// fixed finder count. Both trees should scale linearly in batch --
// striping does not make a single call's K blobs parallel -- so this is
// mainly a control: a difference in SHAPE here would mean the harness
// is measuring something other than what it claims.
func BenchmarkFindMissingRefreshBatch(b *testing.B) {
	for _, batch := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkFindMissingRefresh(b, 8, batch, benchCopyDelay)
		})
	}
}

// BenchmarkGetDuringRefresh measures the incident's actual symptom
// rather than its mechanism: Get() latency while refresh traffic is in
// flight. The 2026-09-04 page was a read-SLI collapse, so this is the
// number that maps onto what the on-call saw.
func BenchmarkGetDuringRefresh(b *testing.B) {
	for _, finders := range []int{0, 1, 4, 16} {
		b.Run(fmt.Sprintf("finders=%d", finders), func(b *testing.B) {
			access, sets, fresh, lbm := benchHarness(finders, 4, benchCopyDelay)
			ctx := context.Background()

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for f := 0; f < finders; f++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						access.FindMissing(ctx, sets[id])
					}
				}(f)
			}

			// Wait for refresh traffic to actually be in flight before
			// timing. The framework's b.N=1 warm-up would otherwise
			// finish before the finders have started, measuring an
			// unloaded Get and tripping the vacuity guard below.
			if finders > 0 {
				deadline := time.Now().Add(10 * time.Second)
				for lbm.allocations.Load() == 0 {
					if time.Now().After(deadline) {
						close(stop)
						wg.Wait()
						b.Fatal("refresh never started")
					}
					time.Sleep(time.Millisecond)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := access.Get(ctx, fresh)
				if _, err := buf.ToByteSlice(benchBlobSizeBytes); err != nil {
					b.StopTimer()
					close(stop)
					wg.Wait()
					b.Fatalf("Get: %v", err)
				}
			}
			b.StopTimer()
			close(stop)
			wg.Wait()

			if finders > 0 && lbm.allocations.Load() == 0 {
				b.Fatal("no refreshes performed: the benchmark is vacuous")
			}
		})
	}
}
