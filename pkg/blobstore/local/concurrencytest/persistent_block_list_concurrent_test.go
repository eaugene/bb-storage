package concurrencytest_test

import (
	"sync"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
)

// TestPersistentBlockListConcurrentFinalizeAndResolve drives many
// concurrent BlockListPutFinalizer calls together with concurrent
// BlockReferenceResolver reads. Without internal synchronisation in
// PersistentBlockList, the finalizer's epochHashSeeds slice append
// races with the resolver's slice read and the per-block
// writtenOffsetBytes update races between two finalizers, both of
// which trip the race detector under `go test -race`.
//
// Run as:
//
//	go test -race -run TestPersistentBlockListConcurrentFinalizeAndResolve \
//	    ./pkg/blobstore/local/
func TestPersistentBlockListConcurrentFinalizeAndResolve(t *testing.T) {
	const (
		blockSize    = 1 << 20 // 1 MiB per block
		blobSize     = 256
		blobsPerProd = 60 // 32*60*256 = 480KB, fits in one block
		producers    = 32
		readers      = 16
	)

	blockAllocator := local.NewInMemoryBlockAllocator(blockSize)
	blockList, _ := local.NewPersistentBlockList(blockAllocator, 0, nil)

	// Seed a single block so producers have somewhere to put.
	if err := blockList.PushBack(); err != nil {
		t.Fatalf("seed PushBack: %v", err)
	}

	// Seed an epoch with a single put. This makes the resolver
	// slices non-empty so readers can call them validly. Real
	// callers never invoke the resolver on an empty BlockList
	// because the KeyLocationMap.Get would return NotFound first.
	{
		writer := blockList.Put(0, blobSize)
		finalizer := writer(buffer.NewValidatedBufferFromByteSlice(make([]byte, blobSize)))
		if _, err := finalizer(); err != nil {
			t.Fatalf("seed finalizer: %v", err)
		}
	}

	// Producers: take a slot, allocate, copy, finalize. Each
	// producer races with peers on writtenOffsetBytes and may
	// trigger the epochHashSeeds append branch.
	var producersWg sync.WaitGroup
	for p := 0; p < producers; p++ {
		producersWg.Add(1)
		go func() {
			defer producersWg.Done()
			data := make([]byte, blobSize)
			for i := 0; i < blobsPerProd; i++ {
				writer := blockList.Put(0, blobSize)
				finalizer := writer(buffer.NewValidatedBufferFromByteSlice(data))
				if _, err := finalizer(); err != nil {
					t.Errorf("finalizer: %v", err)
					return
				}
			}
		}()
	}

	// Readers: hammer the resolver methods. Use a fresh
	// BlockReference each time so they hit different slots.
	stop := make(chan struct{})
	var readersWg sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// BlockReferenceToBlockIndex reads
				// epochHashSeeds[epochIndex]; concurrent
				// slice append by the finalizer's append
				// branch is the documented race.
				blockList.BlockReferenceToBlockIndex(local.BlockReference{
					EpochID:        0,
					BlocksFromLast: 0,
				})
				// BlockIndexToBlockReference reads the
				// last entry of the same slice.
				blockList.BlockIndexToBlockReference(0)
			}
		}()
	}

	producersWg.Wait()
	close(stop)
	readersWg.Wait()
}
