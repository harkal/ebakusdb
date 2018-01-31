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

// BufferAllocator allocates memory in a preallocated buffer
type BufferAllocator struct {
	buffer        *[maxBufferSize]byte
	firstFreeByte uint64
}

// NewBufferAllocator created a new buffer allocator
func NewBufferAllocator(buf []byte) (*BufferAllocator, error) {
	if len(buf)&alignmentBytesMinusOne != 0 {
		return nil, ErrInvalidSize
	}
	buffer := &BufferAllocator{
		buffer: (*[maxBufferSize]byte)(unsafe.Pointer(&buf[0])),
	}
	return buffer, nil
}

func (b *BufferAllocator) GetPtr(pos uint64) unsafe.Pointer {
	return unsafe.Pointer(&b.buffer[pos])
}

// Allocate a new buffer of specific size
func (b *BufferAllocator) Allocate(size uint64) (uint64, error) {
	if size == 0 {
		return 0, ErrInvalidSize
	}

	if b.firstFreeByte+size > uint64(len(b.buffer)) {
		return 0, ErrOutOfMemory
	}

	p := b.firstFreeByte
	b.firstFreeByte += size

	// Ensure alignement
	if b.firstFreeByte%alignmentBytesMinusOne != 0 {
		b.firstFreeByte += alignmentBytes
		b.firstFreeByte &= ^uint64(alignmentBytesMinusOne)
	}

	return p, nil
}
