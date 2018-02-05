package balloc

import (
	"errors"
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
	bufferPtr  unsafe.Pointer
	bufferSize uint64
	header     *header
}

const magic uint32 = 0xca01af01

type header struct {
	magic         uint32
	firstFreeByte uint64
	TotalUsed     uint64
}

// NewBufferAllocator created a new buffer allocator
func NewBufferAllocator(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64) (*BufferAllocator, error) {
	if bufSize&alignmentBytesMinusOne != 0 {
		return nil, ErrInvalidSize
	}

	buffer := &BufferAllocator{
		bufferPtr:  bufPtr,
		bufferSize: bufSize,
	}

	firstFree = alignSize(firstFree)
	buffer.header = (*header)(unsafe.Pointer(uintptr(bufPtr) + uintptr(firstFree)))

	if buffer.header.magic != magic {
		firstFree += uint64(unsafe.Sizeof(*buffer.header))

		buffer.header.firstFreeByte = uint64(alignSize(firstFree))
		buffer.header.TotalUsed = 0
	}

	return buffer, nil
}

func (b *BufferAllocator) SetBuffer(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64) {
	b.bufferPtr = bufPtr
	b.bufferSize = bufSize
	b.header = (*header)(unsafe.Pointer(uintptr(bufPtr) + uintptr(firstFree)))
}

func (b *BufferAllocator) GetFree() uint64 {
	return b.bufferSize - b.header.TotalUsed
}

func (b *BufferAllocator) GetCapacity() uint64 {
	return b.bufferSize
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
		return 0, ErrOutOfMemory
	}

	// Ensure alignement
	size = alignSize(size)

	p := b.header.firstFreeByte
	b.header.firstFreeByte += size

	b.header.TotalUsed += size

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset uint64, size uint64) error {
	//fmt.Printf("- Deallocate %d bytes\n", size)
	size = alignSize(size)
	b.header.TotalUsed -= size
	return nil
}

func alignSize(size uint64) uint64 {
	if size&alignmentBytesMinusOne != 0 {
		size += alignmentBytes
		size &= ^uint64(alignmentBytesMinusOne)
	}
	return size
}
