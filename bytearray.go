package ebakusdb

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/ebakus/ebakusdb/balloc"
)

var (
	ErrInvalidSize = errors.New("Input data size is invalid")
)

const maxDataSize = 0x9C4000

var bytesCount int

func newBytes(mm balloc.MemoryManager, size uint32) (*ByteArray, []byte, error) {
	offset, err := mm.Allocate(uint64(unsafe.Sizeof(int(0))+uintptr(size)), false)
	if err != nil {
		return nil, nil, err
	}
	aPtr := &ByteArray{Offset: offset, Size: size}
	*aPtr.getBytesRefCount(mm) = 1
	a := aPtr.getBytes(mm)
	//bytesCount++
	//println("ByteArray alloc", bytesCount)
	return aPtr, a, nil
}

func newBytesFromSlice(mm balloc.MemoryManager, data []byte) *ByteArray {
	aPtr, a, err := newBytes(mm, uint32(len(data)))
	if err != nil {
		panic(fmt.Sprintf("%s (allocating: %d bytes, used: %d, free: %d)", err, len(data), mm.GetUsed(), mm.GetFree()))
	}
	copy(a, data)

	return aPtr
}

func (bPtr *ByteArray) cloneBytes(mm balloc.MemoryManager) (*ByteArray, error) {
	newBPtr, newB, err := newBytes(mm, bPtr.Size)
	if err != nil {
		return nil, err
	}

	old := bPtr.getBytes(mm)

	copy(newB, old)

	return newBPtr, nil
}

func checkBytesLength(data []byte) error {
	if uint64(unsafe.Sizeof(int(0))+uintptr(len(data))) > maxDataSize {
		// if len(data) > maxDataSize {
		return ErrInvalidSize
	}
	return nil
}

func (b *ByteArray) checkBytesLength() error {
	if uint64(unsafe.Sizeof(int(0))+uintptr(b.Size)) > maxDataSize {
		// if len(data) > maxDataSize {
		return ErrInvalidSize
	}
	return nil
}

func (b *ByteArray) getBytes(mm balloc.MemoryManager) []byte {
	//println("getBytes", b.Offset, "of count", *b.getBytesRefCount(mm), "value:", string((*[0x7fffff]byte)(mm.GetPtr(b.Offset + uint64(unsafe.Sizeof(int(0)))))[:b.Size]))
	return (*[maxDataSize]byte)(mm.GetPtr(b.Offset + uint64(unsafe.Sizeof(int(0)))))[:b.Size]
}

func (b *ByteArray) getBytesRefCount(mm balloc.MemoryManager) *int32 {
	return (*int32)(mm.GetPtr(b.Offset))
}

func (b *ByteArray) Retain(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}
	//println("Retain", b.Offset, "of count", *b.getBytesRefCount(mm), string(b.getBytes(mm)))
	if *b.getBytesRefCount(mm) == 0 {
		panic("inc zero refs")
	}
	atomic.AddInt32(b.getBytesRefCount(mm), 1)
}

func (b *ByteArray) Release(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}

	count := b.getBytesRefCount(mm)

	if atomic.AddInt32(count, -1) == 0 {
		if err := mm.Deallocate(b.Offset, uint64(b.Size)+uint64(unsafe.Sizeof(int(0)))); err != nil {
			panic(err)
		}
		//bytesCount--
		//println("ByteArray release", bytesCount)
	}

	b.Offset = 0
	b.Size = 0
}
