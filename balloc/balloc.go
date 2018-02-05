package balloc

import (
	"errors"
	"fmt"
	"unsafe"
)

var (
	ErrOutOfMemory = errors.New("Not enough space allocating memory")
	ErrInvalidSize = errors.New("The requested size is invalid")
)

const maxBufferSize = 0x8000000000
const alignmentBytes = 8
const alignmentBytesMinusOne = alignmentBytes - 1

type MemoryManager interface {
	Allocate(size uint64) (uint64, error)
	Deallocate(pos, size uint64) error

	GetPtr(pos uint64) unsafe.Pointer
}

// BufferAllocator allocates memory in a preallocated buffer
type BufferAllocator struct {
	bufferRef  []byte
	bufferPtr  unsafe.Pointer
	bufferSize uint64
	header     *header

	Grow func(size uint64) error
}

const magic uint32 = 0xca01af01

type header struct {
	magic         uint32
	firstFreeByte uint64
	TotalFree     uint64
}

// NewBufferAllocator created a new buffer allocator
func NewBufferAllocator(buf []byte, firstFree uint64) (*BufferAllocator, error) {
	if len(buf)&alignmentBytesMinusOne != 0 {
		return nil, ErrInvalidSize
	}

	buffer := &BufferAllocator{
		bufferRef:  buf,
		bufferPtr:  unsafe.Pointer(&buf[0]),
		bufferSize: uint64(len(buf)),
	}

	firstFree = alignSize(firstFree)
	buffer.header = (*header)(unsafe.Pointer(&buf[firstFree]))

	if buffer.header.magic != magic {
		firstFree += uint64(unsafe.Sizeof(*buffer.header))

		buffer.header.firstFreeByte = uint64(alignSize(firstFree))
		buffer.header.TotalFree = uint64(len(buf))
	}

	return buffer, nil
}

const megaByte = 1024 * 1024
const gigaByte = 1024 * megaByte

func (b *BufferAllocator) growBuffer(size uint64) error {
	var newSize = b.bufferSize

	for newSize < size {
		if b.bufferSize < gigaByte {
			newSize *= 2
		} else if b.bufferSize >= gigaByte {
			newSize += gigaByte
		}
	}

	if err := b.Grow(newSize); err != nil {
		return err
	}

	b.bufferSize = newSize

	return nil
}

func (b *BufferAllocator) GetFree() uint64 {
	return b.header.TotalFree
}

func (b *BufferAllocator) GetPtr(pos uint64) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(pos))
}

// Allocate a new buffer of specific size
func (b *BufferAllocator) Allocate(size uint64) (uint64, error) {
	//fmt.Printf("+ allocate %d bytes\n", size)
	if size == 0 {
		return 0, ErrInvalidSize
	}

	if b.header.firstFreeByte+size > b.bufferSize {
		fmt.Printf("+ GROW %d bytes\n", b.header.firstFreeByte+size)
		if err := b.growBuffer(b.header.firstFreeByte + size); err != nil {
			return 0, ErrOutOfMemory
		}
	}

	// Ensure alignement
	size = alignSize(size)

	p := b.header.firstFreeByte
	b.header.firstFreeByte += size

	b.header.TotalFree -= size

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset uint64, size uint64) error {
	//fmt.Printf("- Deallocate %d bytes\n", size)
	size = alignSize(size)
	b.header.TotalFree += size
	return nil
}

func alignSize(size uint64) uint64 {
	if size&alignmentBytesMinusOne != 0 {
		size += alignmentBytes
		size &= ^uint64(alignmentBytesMinusOne)
	}
	return size
}
