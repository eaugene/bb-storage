//go:build !linux

package blockdevice

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newBlockDeviceFromDeviceDirect(path string) (BlockDevice, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Direct I/O is only supported on Linux")
}

func newBlockDeviceFromFileDirect(path string, minimumSizeBytes int, zeroInitialize bool) (BlockDevice, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Direct I/O is only supported on Linux")
}
