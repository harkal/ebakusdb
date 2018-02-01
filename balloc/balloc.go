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
	Deallocate(pos uint64) error

	GetPtr(pos uint64) unsafe.Pointer
}

// BufferAllocator allocates memory in a preallocated buffer
type BufferAllocator struct {
	bufferRef     []byte
	buffer        *[maxBufferSize]byte
	bufferSize    uint64
	firstFreeByte uint64

	TotalFree uint64
}

// NewBufferAllocator created a new buffer allocator
func NewBufferAllocator(buf []byte) (*BufferAllocator, error) {
	if len(buf)&alignmentBytesMinusOne != 0 {
		return nil, ErrInvalidSize
	}
	buffer := &BufferAllocator{
		bufferRef:  buf,
		buffer:     (*[maxBufferSize]byte)(unsafe.Pointer(&buf[0])),
		bufferSize: uint64(len(buf)),
		TotalFree:  uint64(len(buf)),
	}
	return buffer, nil
}

func (b *BufferAllocator) GetPtr(pos uint64) unsafe.Pointer {
	return unsafe.Pointer(&b.buffer[pos])
}

func alignSize(size uint64) uint64 {
	if size&alignmentBytesMinusOne != 0 {
		size += alignmentBytes
		size &= ^uint64(alignmentBytesMinusOne)
	}
	return size
}

// Allocate a new buffer of specific size
func (b *BufferAllocator) Allocate(size uint64) (uint64, error) {
	if size == 0 {
		return 0, ErrInvalidSize
	}

	if b.firstFreeByte+size > b.bufferSize {
		return 0, ErrOutOfMemory
	}

	// Ensure alignement
	size = alignSize(size)

	p := b.firstFreeByte
	b.firstFreeByte += size

	b.TotalFree -= size

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset uint64, size uint64) error {
	b.TotalFree += alignSize(size)
	return nil
}
