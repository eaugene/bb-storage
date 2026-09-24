package concurrencytest_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// trackingLocationBlobMap is countingLocationBlobMap plus a record of
// how many refresh copies were in flight simultaneously. The copy is
// exactly the region the refresh semaphore is meant to bound, so the
// high-water mark is the property under test.
type trackingLocationBlobMap struct {
	lock       sync.Mutex
	nextOffset int64

	inFlight     atomic.Int64
	peakInFlight atomic.Int64
}

func (lbm *trackingLocationBlobMap) Get(location local.Location) (local.LocationBlobGetter, bool) {
	needsRefresh := location.BlockIndex == 0
	return func(digest.Digest) buffer.Buffer {
		return buffer.NewValidatedBufferFromByteSlice(make([]byte, location.SizeBytes))
	}, needsRefresh
}

func (lbm *trackingLocationBlobMap) Put(sizeBytes int64) (local.LocationBlobPutWriter, error) {
	lbm.lock.Lock()
	offsetBytes := lbm.nextOffset
	lbm.nextOffset += sizeBytes
	lbm.lock.Unlock()

	return func(b buffer.Buffer) local.LocationBlobPutFinalizer {
		b.Discard()

		// Model the copy, and record concurrency across it.
		current := lbm.inFlight.Add(1)
		for {
			peak := lbm.peakInFlight.Load()
			if current <= peak || lbm.peakInFlight.CompareAndSwap(peak, current) {
				break
			}
		}
		time.Sleep(refreshCopyDelay)
		lbm.inFlight.Add(-1)

		return func() (local.Location, error) {
			return local.Location{
				BlockIndex:  1,
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			}, nil
		}
	}, nil
}

// TestFindMissingRefreshConcurrencyLimit pins the bandwidth property
// that the striped refresh lock would otherwise have dropped.
//
// Upstream's single refreshLock served two purposes: deduplicating
// refreshes of the same blob, and limiting refresh bandwidth to one
// thread. Striping preserves the first (see TestFindMissingRefreshDedup)
// but not the second, so FlatBlobAccess carries an explicit semaphore.
// This test asserts that the semaphore is actually enforced, and that
// it is not so conservative that refresh is still serialised.
//
// Each finder refreshes a disjoint set of blobs, so no refresh is
// deduplicated away and every finder does real copy work. Without the
// semaphore the observed peak rises to the number of finders.
func TestFindMissingRefreshConcurrencyLimit(t *testing.T) {
	const (
		finders           = 16
		blobsPerFinder    = 8
		blobSizeBytes     = 256
		unlimitedByStripe = 0
	)

	for _, tc := range []struct {
		name               string
		refreshConcurrency int
		wantPeakAtMost     int64
		wantPeakAtLeast    int64
	}{
		{
			// The historical behaviour, now opt-in rather than
			// mandatory: refresh is fully serialised.
			name:               "SingleThreaded",
			refreshConcurrency: 1,
			wantPeakAtMost:     1,
			wantPeakAtLeast:    1,
		},
		{
			// The limit must bind: 16 finders are pushing, so
			// without the semaphore the peak would exceed 4.
			name:               "Bounded",
			refreshConcurrency: 4,
			wantPeakAtMost:     4,
			wantPeakAtLeast:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyLocationMap := &stubKeyLocationMap{m: map[local.Key]local.Location{}}
			locationBlobMap := &trackingLocationBlobMap{}

			// Seed every blob into block 0, i.e. in need of refresh,
			// and give each finder its own disjoint digest set.
			digestSets := make([]digest.Set, 0, finders)
			for f := 0; f < finders; f++ {
				setBuilder := digest.NewSetBuilder(blobsPerFinder)
				for i := 0; i < blobsPerFinder; i++ {
					d := makeDigest(uint64(f*blobsPerFinder+i), blobSizeBytes)
					setBuilder.Add(d)
					keyLocationMap.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{
						BlockIndex:  0,
						OffsetBytes: int64(i) * blobSizeBytes,
						SizeBytes:   blobSizeBytes,
					}
				}
				digestSets = append(digestSets, setBuilder.Build())
			}

			var globalLock sync.RWMutex
			access := local.NewFlatBlobAccess(
				keyLocationMap,
				locationBlobMap,
				digest.KeyWithoutInstance,
				&globalLock,
				tc.refreshConcurrency,
				"limit",
				capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
			)

			ctx := context.Background()
			start := make(chan struct{})
			var wg sync.WaitGroup
			errs := make([]error, finders)
			for f := 0; f < finders; f++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					<-start
					_, errs[id] = access.FindMissing(ctx, digestSets[id])
				}(f)
			}
			close(start)
			wg.Wait()

			for id, err := range errs {
				if err != nil {
					t.Fatalf("finder %d: FindMissing: %v", id, err)
				}
			}

			peak := locationBlobMap.peakInFlight.Load()
			if peak > tc.wantPeakAtMost {
				t.Errorf("peak concurrent refreshes = %d, want at most %d; "+
					"the refresh semaphore is not bounding the copy",
					peak, tc.wantPeakAtMost)
			}
			if peak < tc.wantPeakAtLeast {
				t.Errorf("peak concurrent refreshes = %d, want at least %d; "+
					"refresh is more serialised than configured",
					peak, tc.wantPeakAtLeast)
			}
			if remaining := locationBlobMap.inFlight.Load(); remaining != 0 {
				t.Errorf("in-flight refreshes = %d after all finders returned, want 0", remaining)
			}
		})
	}
}

// TestRefreshConcurrencyRespectsContext asserts that a caller blocked
// waiting for a refresh slot gives up when its context is cancelled,
// rather than holding the stripe until a slot frees up.
func TestRefreshConcurrencyRespectsContext(t *testing.T) {
	const (
		finders        = 8
		blobsPerFinder = 4
		blobSizeBytes  = 256
	)

	keyLocationMap := &stubKeyLocationMap{m: map[local.Key]local.Location{}}
	locationBlobMap := &trackingLocationBlobMap{}

	digestSets := make([]digest.Set, 0, finders)
	for f := 0; f < finders; f++ {
		setBuilder := digest.NewSetBuilder(blobsPerFinder)
		for i := 0; i < blobsPerFinder; i++ {
			d := makeDigest(uint64(1000+f*blobsPerFinder+i), blobSizeBytes)
			setBuilder.Add(d)
			keyLocationMap.m[local.NewKeyFromString(d.GetKey(digest.KeyWithoutInstance))] = local.Location{
				BlockIndex:  0,
				OffsetBytes: int64(i) * blobSizeBytes,
				SizeBytes:   blobSizeBytes,
			}
		}
		digestSets = append(digestSets, setBuilder.Build())
	}

	var globalLock sync.RWMutex
	access := local.NewFlatBlobAccess(
		keyLocationMap,
		locationBlobMap,
		digest.KeyWithoutInstance,
		&globalLock,
		1,
		"cancel",
		capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}),
	)

	// With a single slot and a modelled copy delay, most of these
	// finders are queued behind the slot when the context is
	// cancelled. They must all return rather than block.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for f := 0; f < finders; f++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			//nolint:errcheck // Either outcome is valid; the assertion is that it returns.
			access.FindMissing(ctx, digestSets[id])
		}(f)
	}
	time.Sleep(2 * refreshCopyDelay)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("FindMissing callers did not return after context cancellation; " +
			"a caller is blocked on the refresh semaphore while holding a stripe")
	}
}
