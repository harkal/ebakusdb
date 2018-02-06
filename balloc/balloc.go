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
	Allocate(size uint64, zero bool) (uint64, error)
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
	magic          uint32
	firstFreeChunk uint64
	TotalUsed      uint64
}

type chunk struct {
	nextFree uint64 // zero if occupied
	prevFree uint64
	size     uint64
}

var chunkSize = uint64(unsafe.Sizeof(chunk{}))

type allocPreable struct {
	size uint64
}

var allocPreableSize uint64 = uint64(unsafe.Sizeof(allocPreable{}))

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

		buffer.header.firstFreeChunk = uint64(alignSize(firstFree))
		buffer.header.TotalUsed = 0

		c := buffer.getChunk(buffer.header.firstFreeChunk)
		c.size = bufSize - buffer.header.firstFreeChunk
		c.nextFree = 0
	}

	return buffer, nil
}

func (b *BufferAllocator) SetBuffer(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64) {
	oldSize := b.bufferSize

	firstFree = alignSize(firstFree)

	b.bufferPtr = bufPtr
	b.bufferSize = bufSize
	b.header = (*header)(unsafe.Pointer(uintptr(bufPtr) + uintptr(firstFree)))

	// find last free chunk
	chunkPos := b.header.firstFreeChunk
	c := b.getChunk(chunkPos)
	for c.nextFree != 0 {
		chunkPos = c.nextFree
		c = b.getChunk(chunkPos)
	}

	nc := b.getChunk(oldSize)
	c.nextFree = oldSize
	nc.prevFree = chunkPos
	nc.nextFree = 0
	nc.size = bufSize - oldSize
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
func (b *BufferAllocator) Allocate(size uint64, zero bool) (uint64, error) {
	//fmt.Printf("+ allocate %d bytes\n", size)
	if size == 0 {
		return 0, ErrInvalidSize
	}

	size += allocPreableSize

	// Ensure alignement
	size = alignSize(size)

	if b.header.firstFreeChunk+size > b.bufferSize {
		return 0, ErrOutOfMemory
	}

	chunkPos := b.header.firstFreeChunk
	var c *chunk
	for chunkPos != 0 {
		c = b.getChunk(chunkPos)
		if c.size >= size+chunkSize {
			break
		}
		chunkPos = c.nextFree
	}

	if chunkPos == 0 {
		return 0, ErrOutOfMemory
	}

	c.size -= size

	newFree := chunkPos + size
	nc := b.getChunk(newFree)
	if nc != nil {
		*nc = *c
	} else {
		newFree = 0
	}

	prev := b.getChunk(c.prevFree)
	if prev != nil {
		prev.nextFree = newFree
	} else {
		b.header.firstFreeChunk = newFree
	}
	next := b.getChunk(c.nextFree)
	if next != nil {
		next.prevFree = newFree
	}

	p := chunkPos + allocPreableSize

	if zero {
		buf := (*[maxBufferSize]uint64)(b.GetPtr(p))
		s := (size - allocPreableSize) / uint64(unsafe.Sizeof(uint64(0)))
		for i := uint64(0); i < s; i++ {
			buf[i] = 0
		}
	}

	b.header.TotalUsed += size

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset uint64, size uint64) error {
	//fmt.Printf("- Deallocate %d bytes\n", size)
	size += allocPreableSize
	size = alignSize(size)
	b.header.TotalUsed -= size
	return nil
}

func (b *BufferAllocator) getChunk(offset uint64) *chunk {
	if offset == 0 || offset > b.bufferSize-uint64(unsafe.Sizeof(chunk{})) {
		return nil
	}
	return (*chunk)(unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(offset)))
}

func alignSize(size uint64) uint64 {
	if size&alignmentBytesMinusOne != 0 {
		size += alignmentBytes
		size &= ^uint64(alignmentBytesMinusOne)
	}
	return size
}
