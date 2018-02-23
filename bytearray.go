package ebakusdb

import (
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

func newBytes(mm balloc.MemoryManager, size uint32) (*ByteArray, []byte, error) {
	offset, err := mm.Allocate(uint64(unsafe.Sizeof(int(0))+uintptr(size)), false)
	if err != nil {
		return nil, nil, err
	}
	aPtr := &ByteArray{Offset: offset, Size: size}
	aPtr.Retain(mm)
	a := aPtr.getBytes(mm)
	return aPtr, a, nil
}

func newBytesFromSlice(mm balloc.MemoryManager, data []byte) *ByteArray {
	aPtr, a, err := newBytes(mm, uint32(len(data)))
	if err != nil {
		panic(err)
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

func (b *ByteArray) getBytes(mm balloc.MemoryManager) []byte {
	return (*[0x7fffff]byte)(mm.GetPtr(b.Offset + uint64(unsafe.Sizeof(int(0)))))[:b.Size]
}

func (b *ByteArray) getBytesRefCount(mm balloc.MemoryManager) *int {
	return (*int)(mm.GetPtr(b.Offset))
}

func (b *ByteArray) Retain(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}
	*b.getBytesRefCount(mm)++
}

func (b *ByteArray) Release(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}
	count := b.getBytesRefCount(mm)
	*count--
	if *count == 0 {
		mm.Deallocate(b.Offset)
	}
}
