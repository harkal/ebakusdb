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
	*aPtr.getBytesRefCount(mm) = 1
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
	//println("getBytes", b.Offset, "of count", *b.getBytesRefCount(mm), "value:", string((*[0x7fffff]byte)(mm.GetPtr(b.Offset + uint64(unsafe.Sizeof(int(0)))))[:b.Size]))
	return (*[0x7fffff]byte)(mm.GetPtr(b.Offset + uint64(unsafe.Sizeof(int(0)))))[:b.Size]
}

func (b *ByteArray) getBytesRefCount(mm balloc.MemoryManager) *int {
	return (*int)(mm.GetPtr(b.Offset))
}

func (b *ByteArray) Retain(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}
	//println("Retain", b.Offset, "of count", *b.getBytesRefCount(mm), string(b.getBytes(mm)))
	if *b.getBytesRefCount(mm) == 0 {
		panic("inc zero refs")
	}
	*b.getBytesRefCount(mm)++
}

func (b *ByteArray) Release(mm balloc.MemoryManager) {
	if b.Offset == 0 {
		return
	}
	count := b.getBytesRefCount(mm)
	//println("Release", b.Offset, "of count", *count, string(b.getBytes(mm)))
	*count--

	if *count == 0 {
		if err := mm.Deallocate(b.Offset, uint64(b.Size)+uint64(unsafe.Sizeof(int(0)))); err != nil {
			panic(err)
		}
	}
}
