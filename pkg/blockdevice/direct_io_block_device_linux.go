//go:build linux

package blockdevice

import (
	"io"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type directIOBlockDevice struct {
	fd            int
	sizeBytes     int64
	sectorSize    int
}

// newDirectIOBlockDevice creates a BlockDevice that uses O_DIRECT for
// both reads and writes, bypassing the kernel page cache entirely.
//
// This is useful in containerized environments where page cache is
// attributed to the cgroup's memory limit. For large block devices,
// mmap-based access can accumulate 100+ GiB of page cache, competing
// with the application heap and causing OOM kills.
//
// All I/O buffers are aligned to the sector size as required by
// O_DIRECT.
func newDirectIOBlockDevice(fd int, sizeBytes int64, sectorSize int) BlockDevice {
	return &directIOBlockDevice{
		fd:         fd,
		sizeBytes:  sizeBytes,
		sectorSize: sectorSize,
	}
}

// allocAligned allocates a byte slice aligned to the given alignment.
// O_DIRECT requires buffers to be aligned to the device sector size.
func allocAligned(size, alignment int) []byte {
	buf := make([]byte, size+alignment)
	offset := alignment - int(uintptr(unsafe.Pointer(&buf[0]))%uintptr(alignment))
	if offset == alignment {
		offset = 0
	}
	return buf[offset : offset+size]
}

func (bd *directIOBlockDevice) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off >= bd.sizeBytes {
		return 0, io.EOF
	}

	sectorSize := bd.sectorSize
	nTotal := 0

	// If the offset is not sector-aligned, read the partial leading sector.
	startAlign := int(off % int64(sectorSize))
	if startAlign != 0 {
		alignedOff := off - int64(startAlign)
		buf := allocAligned(sectorSize, sectorSize)
		n, err := unix.Pread(bd.fd, buf, alignedOff)
		if err != nil {
			return 0, err
		}
		if n < sectorSize {
			return 0, io.ErrUnexpectedEOF
		}
		copied := copy(p, buf[startAlign:])
		nTotal += copied
		p = p[copied:]
		off += int64(copied)
	}

	// Read full sectors directly into aligned buffers.
	for len(p) >= sectorSize {
		// Determine chunk size (read in chunks to avoid huge allocations).
		chunkSize := len(p) - (len(p) % sectorSize)
		if chunkSize > 4*1024*1024 {
			chunkSize = 4 * 1024 * 1024
		}
		buf := allocAligned(chunkSize, sectorSize)
		n, err := unix.Pread(bd.fd, buf, off)
		if n > 0 {
			copied := copy(p[:n], buf[:n])
			nTotal += copied
			p = p[copied:]
			off += int64(copied)
		}
		if err != nil {
			if nTotal > 0 {
				return nTotal, err
			}
			return 0, err
		}
		if n == 0 {
			break
		}
	}

	// Read the partial trailing sector.
	if len(p) > 0 {
		buf := allocAligned(sectorSize, sectorSize)
		n, err := unix.Pread(bd.fd, buf, off)
		if n > 0 {
			copied := copy(p, buf[:n])
			nTotal += copied
		}
		if err != nil && nTotal == 0 {
			return 0, err
		}
	}

	if nTotal < len(p)+nTotal {
		// This shouldn't happen in normal flow, but handle EOF.
	}
	return nTotal, nil
}

func (bd *directIOBlockDevice) WriteAt(p []byte, off int64) (int, error) {
	sectorSize := bd.sectorSize
	nTotal := 0

	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > 4*1024*1024 {
			chunkSize = 4 * 1024 * 1024
		}
		// Round up to sector boundary for alignment.
		alignedSize := ((chunkSize + sectorSize - 1) / sectorSize) * sectorSize
		buf := allocAligned(alignedSize, sectorSize)
		copy(buf, p[:chunkSize])

		n, err := unix.Pwrite(bd.fd, buf[:alignedSize], off)
		if err != nil {
			return nTotal, err
		}
		written := n
		if written > chunkSize {
			written = chunkSize
		}
		nTotal += written
		p = p[written:]
		off += int64(n)
	}
	return nTotal, nil
}

func (bd *directIOBlockDevice) Sync() error {
	return unix.Fsync(bd.fd)
}

func (bd *directIOBlockDevice) Close() error {
	return unix.Close(bd.fd)
}
