//go:build linux

package blockdevice

import (
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

// NewBlockDeviceFromDeviceDirect opens a block device with O_DIRECT,
// bypassing the kernel page cache. Reads and writes use pread/pwrite
// with sector-aligned buffers instead of memory-mapped I/O.
func NewBlockDeviceFromDeviceDirect(path string) (BlockDevice, int, int64, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_DIRECT, 0)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open device node %#v with O_DIRECT", path)
	}

	var sectorSizeBytes int32
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKBSZGET, uintptr(unsafe.Pointer(&sectorSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain block size of device node %#v", path)
	}
	var deviceSizeBytes int64
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&deviceSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain size of device node %#v", path)
	}

	bd := newDirectIOBlockDevice(fd, deviceSizeBytes, int(sectorSizeBytes))
	return bd, int(sectorSizeBytes), deviceSizeBytes / int64(sectorSizeBytes), nil
}

// NewBlockDeviceFromFileDirect opens a file-backed block device with
// O_DIRECT, bypassing the kernel page cache.
func NewBlockDeviceFromFileDirect(path string, minimumSizeBytes int, zeroInitialize bool) (BlockDevice, int, int64, error) {
	flags := unix.O_CREAT | unix.O_RDWR | unix.O_DIRECT
	if zeroInitialize {
		flags |= unix.O_TRUNC
	}
	fd, err := unix.Open(path, flags, 0o666)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open file %#v with O_DIRECT", path)
	}

	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain size of file %#v", path)
	}
	sectorSizeBytes := max(int(stat.Blksize), unix.Getpagesize())
	sectorCount := int64((uint64(minimumSizeBytes) + uint64(sectorSizeBytes) - 1) / uint64(sectorSizeBytes))
	sizeBytes := int64(sectorSizeBytes) * sectorCount

	if err := unix.Ftruncate(fd, sizeBytes); err != nil {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to truncate file %#v to %d bytes", path, sizeBytes)
	}

	bd := newDirectIOBlockDevice(fd, sizeBytes, sectorSizeBytes)
	return bd, sectorSizeBytes, sectorCount, nil
}
