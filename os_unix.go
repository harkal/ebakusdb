package ebakusdb

import (
	"fmt"
	"syscall"
	"unsafe"
)

func (db *DB) mmap(sz int) error {
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	_, _, e := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), syscall.MADV_RANDOM)
	if e != 0 {
		return fmt.Errorf("madvise error: %s", e)
	}

	db.bufferRef = b
	db.buffer = (*[0x9000000000]byte)(unsafe.Pointer(&b[0]))
	db.bufferSize = uint64(sz)
	return nil
}

func (db *DB) munmap() error {
	if db.bufferRef == nil {
		return nil
	}

	err := syscall.Munmap(db.bufferRef)
	db.bufferRef = nil
	db.buffer = nil
	db.bufferSize = 0

	return err
}
