//go:build linux

package blockdevice

func newBlockDeviceFromDeviceDirect(path string) (BlockDevice, int, int64, error) {
	return NewBlockDeviceFromDeviceDirect(path)
}

func newBlockDeviceFromFileDirect(path string, minimumSizeBytes int, zeroInitialize bool) (BlockDevice, int, int64, error) {
	return NewBlockDeviceFromFileDirect(path, minimumSizeBytes, zeroInitialize)
}
