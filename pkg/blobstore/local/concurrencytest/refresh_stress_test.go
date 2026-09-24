package concurrencytest_test

// Stress test for the FindMissing() refresh path against the REAL
// storage stack, as a counterpart to the stub-based microbenchmarks in
// refresh_contention_bench_test.go.
//
// The stack assembled here is the production CAS shape:
//
//	slowBlockAllocator (models a disk-backed device)
//	  -> volatileBlockList        (prod: 'persistent' is unset)
//	    -> OldCurrentNewLocationBlobMap
//	      -> hashingKeyLocationMap over an in-memory record array
//	        -> FlatBlobAccess
//
// It reports Get and FindMissing tail latency under mixed traffic and
// byte-verifies every successful read, so it is both a performance A/B
// and a corruption check. It compiles against the baseline and the
// fixed tree alike.
//
// Skipped unless BB_STRESS=1, so it does not slow `bazel test //...`:
//
//	bazel build //pkg/blobstore/local/concurrencytest:benchmark_test
//	BB_STRESS=1 BB_STRESS_FINDERS=16 \
//	  bazel-bin/pkg/blobstore/local/concurrencytest/benchmark_test_/benchmark_test \
//	  -test.run TestRefreshStress -test.v
//
// Tunables (all optional): BB_STRESS_DUR, BB_STRESS_FINDERS,
// BB_STRESS_READERS, BB_STRESS_WRITERS, BB_STRESS_READLAT,
// BB_STRESS_WRITELAT.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
)

// ---------------------------------------------------------------------
// Latency injection
// ---------------------------------------------------------------------

// slowBlockAllocator wraps a BlockAllocator so that block reads and
// writes cost something, modelling a disk-backed block device. Without
// this an in-memory harness copies blobs at memcpy speed and the
// section the refresh lock is held across is too short to observe.
//
// Note the write latency is injected in the BlockPutWriter, i.e. inside
// the copy, which is the half a Block.Get-only harness misses.
type slowBlockAllocator struct {
	inner             local.BlockAllocator
	readLat, writeLat time.Duration
}

func (a *slowBlockAllocator) NewBlock() (local.Block, *pb.BlockLocation, error) {
	b, loc, err := a.inner.NewBlock()
	if err != nil {
		return nil, nil, err
	}
	return &slowBlock{inner: b, readLat: a.readLat, writeLat: a.writeLat}, loc, nil
}

func (a *slowBlockAllocator) NewBlockAtLocation(location *pb.BlockLocation, writeOffsetBytes int64) (local.Block, bool) {
	b, ok := a.inner.NewBlockAtLocation(location, writeOffsetBytes)
	if !ok {
		return nil, false
	}
	return &slowBlock{inner: b, readLat: a.readLat, writeLat: a.writeLat}, true
}

type slowBlock struct {
	inner             local.Block
	readLat, writeLat time.Duration
}

func (b *slowBlock) Get(d digest.Digest, offsetBytes, sizeBytes int64, cb buffer.DataIntegrityCallback) buffer.Buffer {
	time.Sleep(b.readLat)
	return b.inner.Get(d, offsetBytes, sizeBytes, cb)
}

func (b *slowBlock) HasSpace(sizeBytes int64) bool { return b.inner.HasSpace(sizeBytes) }

func (b *slowBlock) Put(sizeBytes int64) local.BlockPutWriter {
	w := b.inner.Put(sizeBytes)
	return func(buf buffer.Buffer) local.BlockPutFinalizer {
		time.Sleep(b.writeLat)
		return w(buf)
	}
}

func (b *slowBlock) Release() { b.inner.Release() }

// countingLocationBlobMapWrapper records how often the stack reported
// that a blob needs refreshing. If this is zero the run proves nothing,
// which is the failure mode that makes ring-buffer harnesses silently
// stop testing refresh once they start self-evicting.
type countingLocationBlobMapWrapper struct {
	local.LocationBlobMap
	refreshNeeded atomic.Int64
}

func (m *countingLocationBlobMapWrapper) Get(location local.Location) (local.LocationBlobGetter, bool) {
	getter, needsRefresh := m.LocationBlobMap.Get(location)
	if needsRefresh {
		m.refreshNeeded.Add(1)
	}
	return getter, needsRefresh
}

// ---------------------------------------------------------------------
// Latency recording
// ---------------------------------------------------------------------

type latencySamples []time.Duration

func (s latencySamples) quantile(q float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	i := int(q * float64(len(s)-1))
	return s[i]
}

func summarise(name string, all []latencySamples) string {
	var merged latencySamples
	for _, s := range all {
		merged = append(merged, s...)
	}
	if len(merged) == 0 {
		return fmt.Sprintf("%-14s (no samples)", name)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return fmt.Sprintf("%-14s n=%-9d p50=%8.3fms  p99=%8.3fms  p99.9=%8.3fms  max=%8.3fms",
		name, len(merged),
		ms(merged.quantile(0.50)), ms(merged.quantile(0.99)),
		ms(merged.quantile(0.999)), ms(merged[len(merged)-1]))
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// ---------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------

func TestRefreshStress(t *testing.T) {
	if os.Getenv("BB_STRESS") == "" {
		t.Skip("set BB_STRESS=1 to run the stress test")
	}

	const (
		blobSizeBytes = 256
		klmEntries    = 65536
	)
	// Ring geometry is what decides whether refresh fires at all. A
	// blob only becomes "old" once currentBlocks+newBlocks blocks have
	// been written past it, so the ring must be small enough relative
	// to the write rate for that to happen many times over during the
	// run. Size it too large and the run is silently vacuous -- the
	// refreshNeeded assertion at the bottom is what catches that.
	var (
		blockSizeBytes = int64(envInt("BB_STRESS_BLOCKKIB", 16)) << 10
		oldBlocks      = envInt("BB_STRESS_OLDBLOCKS", 2)
		currentBlocks  = envInt("BB_STRESS_CURBLOCKS", 8)
		newBlocks      = envInt("BB_STRESS_NEWBLOCKS", 2)
		workingSet     = envInt("BB_STRESS_WORKINGSET", 256)

		duration = envDur("BB_STRESS_DUR", 10*time.Second)
		finders  = envInt("BB_STRESS_FINDERS", 16)
		readers  = envInt("BB_STRESS_READERS", 16)
		writers  = envInt("BB_STRESS_WRITERS", 8)
		readLat  = envDur("BB_STRESS_READLAT", 200*time.Microsecond)
		writeLat = envDur("BB_STRESS_WRITELAT", 50*time.Microsecond)
	)

	allocator := &slowBlockAllocator{
		inner:    local.NewInMemoryBlockAllocator(int(blockSizeBytes)),
		readLat:  readLat,
		writeLat: writeLat,
	}
	blockList := local.NewVolatileBlockList(allocator)
	growthPolicy := local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks)

	inner := local.NewOldCurrentNewLocationBlobMap(
		blockList, growthPolicy, silentStressLogger{}, "stress",
		blockSizeBytes, oldBlocks, newBlocks, 0,
	)
	locationBlobMap := &countingLocationBlobMapWrapper{LocationBlobMap: inner}

	recordsCount := klmEntries
	for !isStressPrime(recordsCount) {
		recordsCount++
	}
	keyLocationMap := local.NewHashingKeyLocationMap(
		local.NewInMemoryLocationRecordArray(recordsCount, inner),
		recordsCount, 0xfeedface, 16, 64, "stress",
	)

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		keyLocationMap, locationBlobMap, digest.KeyWithoutInstance, &globalLock, 0, "stress",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)

	ctx := context.Background()

	// Content is derived from the sequence number, so a reader can
	// recompute what it should have read and detect corruption.
	payload := func(seq uint64) []byte {
		b := make([]byte, blobSizeBytes)
		for i := range b {
			b[i] = byte(seq>>(8*(i%8))) ^ byte(i)
		}
		return b
	}

	populated := make([]digest.Digest, workingSet)
	seqOf := make(map[string]uint64, workingSet)
	for i := range populated {
		d := benchDigest(uint64(i), blobSizeBytes)
		populated[i] = d
		seqOf[d.GetKey(digest.KeyWithoutInstance)] = uint64(i)
		if err := access.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(payload(uint64(i)))); err != nil {
			t.Fatalf("populate Put[%d]: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var puts, hits, misses, finds, corrupt atomic.Uint64

	getLat := make([]latencySamples, readers)
	findLat := make([]latencySamples, finders)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := uint64(10_000_000) + uint64(id)*1_000_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				if err := access.Put(ctx, benchDigest(seq, blobSizeBytes),
					buffer.NewValidatedBufferFromByteSlice(payload(seq))); err == nil {
					puts.Add(1)
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			x := uint64(id)*2654435761 + 1
			for {
				select {
				case <-stop:
					return
				default:
				}
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				d := populated[x%uint64(len(populated))]
				t0 := time.Now()
				data, err := access.Get(ctx, d).ToByteSlice(blobSizeBytes)
				getLat[id] = append(getLat[id], time.Since(t0))
				if err != nil {
					misses.Add(1) // evicted by the churning ring: expected
					continue
				}
				hits.Add(1)
				want := payload(seqOf[d.GetKey(digest.KeyWithoutInstance)])
				for i := range want {
					if data[i] != want[i] {
						corrupt.Add(1)
						break
					}
				}
			}
		}(r)
	}

	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			x := uint64(id)*40503 + 7
			for {
				select {
				case <-stop:
					return
				default:
				}
				sb := digest.NewSetBuilder(16)
				for i := 0; i < 16; i++ {
					x ^= x << 13
					x ^= x >> 7
					x ^= x << 17
					sb.Add(populated[x%uint64(len(populated))])
				}
				t0 := time.Now()
				_, err := access.FindMissing(ctx, sb.Build())
				findLat[id] = append(findLat[id], time.Since(t0))
				if err == nil {
					finds.Add(1)
				}
			}
		}(f)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	fmt.Printf("\n=== TestRefreshStress ===\n")
	fmt.Printf("config        dur=%v finders=%d readers=%d writers=%d readlat=%v writelat=%v\n",
		duration, finders, readers, writers, readLat, writeLat)
	fmt.Printf("ring          block=%dKiB old=%d current=%d new=%d workingset=%d\n",
		blockSizeBytes>>10, oldBlocks, currentBlocks, newBlocks, workingSet)
	fmt.Printf("throughput    puts=%d gets_hit=%d gets_miss=%d finds=%d refresh_needed=%d\n",
		puts.Load(), hits.Load(), misses.Load(), finds.Load(), locationBlobMap.refreshNeeded.Load())
	fmt.Println(summarise("GET", getLat))
	fmt.Println(summarise("FINDMISSING", findLat))
	fmt.Printf("CORRUPT       %d\n\n", corrupt.Load())

	if corrupt.Load() != 0 {
		t.Errorf("data corruption detected on %d reads", corrupt.Load())
	}
	if locationBlobMap.refreshNeeded.Load() == 0 {
		t.Error("refresh never fired: this run measured nothing. Increase the " +
			"block count or the working set so blobs age into the old region " +
			"instead of being evicted outright.")
	}
	if finds.Load() == 0 || hits.Load() == 0 {
		t.Errorf("workload did not exercise all paths: finds=%d hits=%d", finds.Load(), hits.Load())
	}
}

type silentStressLogger struct{}

func (silentStressLogger) Log(err error) {}

func isStressPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}
