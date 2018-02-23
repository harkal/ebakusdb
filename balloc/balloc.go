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
	Allocate(size uint64, zero bool) (uint64, error)
	Deallocate(pos uint64) error

	AllocateNode(zero bool) (uint64, error)
	DeallocateNode(pos uint64) error

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
	bufferStart    uint32
	nodeSize       uint16
	firstFreeNode  uint64
	firstFreeChunk uint64
	TotalUsed      uint64
}

type chunk struct {
	nextFree uint64 // zero if occupied
	size     uint32
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

	firstFreeNode := alignSize(firstFree + 700*1024)
	firstFree = alignSize(firstFree)
	buffer.header = (*header)(unsafe.Pointer(uintptr(bufPtr) + uintptr(firstFree)))

	if buffer.header.magic != magic {
		firstFree += uint64(unsafe.Sizeof(*buffer.header))

		buffer.header.magic = magic
		buffer.header.bufferStart = uint32(alignSize(firstFree))
		buffer.header.firstFreeNode = firstFreeNode
		buffer.header.firstFreeChunk = uint64(alignSize(firstFree))
		buffer.header.TotalUsed = 0

		c := buffer.getChunk(buffer.header.firstFreeChunk)
		c.size = uint32(bufSize - buffer.header.firstFreeChunk)
		c.nextFree = 0
	}

	return buffer, nil
}

func (b *BufferAllocator) SetNodeSize(s uint16) {
	b.header.nodeSize = s
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
	nc.nextFree = 0
	nc.size = uint32(bufSize - oldSize)
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
		if c.size >= uint32(size+chunkSize) {
			break
		}
		chunkPos = c.nextFree
	}

	if chunkPos == 0 {
		return 0, ErrOutOfMemory
	}

	c.size -= uint32(size)

	newFree := chunkPos + size
	nc := b.getChunk(newFree)
	if nc != nil {
		nc.nextFree = c.nextFree
		nc.size = c.size
	} else {
		newFree = 0
	}

	b.header.firstFreeChunk = newFree

	p := chunkPos + allocPreableSize

	if zero {
		buf := (*[maxBufferSize]uint64)(b.GetPtr(p))
		s := (size - allocPreableSize) / uint64(unsafe.Sizeof(uint64(0)))
		for i := uint64(0); i < s; i++ {
			buf[i] = 0
		}
	}

	b.getPreample(p).size = size

	b.header.TotalUsed += size

	//fmt.Printf("+ allocate %d bytes at %d\n", size, chunkPos)

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset uint64) error {
	/*
		size := b.getPreample(offset).size
		offset -= allocPreableSize

		//fmt.Printf("- Deallocate %d bytes at %d\n", size, offset)
		if offset >= uint64(b.header.bufferStart) && offset < b.header.firstFreeChunk {
			nc := b.getChunk(offset)
			nc.prevFree = 0
			nc.nextFree = b.header.firstFreeChunk
			nc.size = size

			chunkNext := b.getChunk(b.header.firstFreeChunk)
			chunkNext.prevFree = offset

			b.header.firstFreeChunk = offset

			b.header.TotalUsed -= size
			return nil
		}

		chunkPrePos := b.header.firstFreeChunk
		var chunkPre *chunk
		for chunkPrePos != 0 {
			chunkPre = b.getChunk(chunkPrePos)
			if rangeContains(chunkPrePos, chunkPre.size, offset) {
				return fmt.Errorf("Memory segmentation error %d already free in (%d,%d)", offset, chunkPrePos, chunkPre.size)
			}

			if offset >= chunkPrePos+chunkPre.size && offset < chunkPre.nextFree {
				break
			}

			chunkPrePos = chunkPre.nextFree
		}

		// Attached next to previous
		if chunkPrePos+chunkPre.size == offset {
			chunkPre.size += size
			b.header.TotalUsed -= size
			return nil
		}

		newFree := offset
		nc := b.getChunk(newFree)
		nc.prevFree = chunkPrePos
		nc.nextFree = chunkPre.nextFree
		nc.size = size

		if chunkPre.nextFree != 0 {
			chunkNext := b.getChunk(chunkPre.nextFree)
			chunkNext.prevFree = newFree
		}
		chunkPre.nextFree = newFree

		b.header.TotalUsed -= size
	*/
	return nil
}

func (b *BufferAllocator) AllocateNode(zero bool) (uint64, error) {
	p := b.header.firstFreeNode

	nodeSize := uint64(b.header.nodeSize)

	b.header.firstFreeNode += nodeSize

	if zero {
		buf := (*[maxBufferSize]uint64)(b.GetPtr(p))
		for i := uint64(0); i < nodeSize; i++ {
			buf[i] = 0
		}
	}

	return p, nil
}

func (b *BufferAllocator) DeallocateNode(pos uint64) error {
	return nil
}

func (b *BufferAllocator) getChunk(offset uint64) *chunk {
	if offset == 0 || offset > b.bufferSize-uint64(unsafe.Sizeof(chunk{})) {
		return nil
	}
	return (*chunk)(unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(offset)))
}

func (b *BufferAllocator) getPreample(offset uint64) *allocPreable {
	offset -= allocPreableSize
	if offset <= 0 || offset > b.bufferSize-allocPreableSize {
		return nil
	}
	return (*allocPreable)(unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(offset)))
}

func (b *BufferAllocator) PrintFreeChunks() {
	chunkPos := b.header.firstFreeChunk
	var c *chunk
	i := 0
	s := uint64(0)
	fmt.Printf("---------------------------------------\n")
	for chunkPos != 0 {
		c = b.getChunk(chunkPos)

		fmt.Printf("Free chunk %d to %d (size: %d)\n", chunkPos, uint32(chunkPos)+c.size-1, c.size)

		chunkPos = c.nextFree
		i++
		s += uint64(c.size)
	}
	fmt.Printf("---------------------------------------\n")
	fmt.Printf("  Total free chunks: %d\n", i)
	fmt.Printf("  Total free memory: %d\n", s)

}

func alignSize(size uint64) uint64 {
	if size&alignmentBytesMinusOne != 0 {
		size += alignmentBytes
		size &= ^uint64(alignmentBytesMinusOne)
	}
	return size
}

func rangeContains(offset, size, testOffset uint64) bool {
	return testOffset >= offset && testOffset < offset+size
}
