package balloc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	GetOffset(p unsafe.Pointer) uint64

	GetUsed() uint64
	GetFree() uint64
}

// BufferAllocator allocates memory in a preallocated buffer
type BufferAllocator struct {
	bufferPtr  unsafe.Pointer
	bufferSize uint64
	header     *header

	mux sync.Mutex
}

const magic uint32 = 0xca01af01

type header struct {
	magic         uint32
	bufferStart   uint32
	pageSize      uint16
	dataWatermark uint64
	freePage      uint64
	TotalUsed     uint64
}

type chunk struct {
	nextFree uint64
	size     uint32
}

var chunkSize = uint64(unsafe.Sizeof(chunk{}))

type allocPreable struct {
	size uint64
}

var allocPreableSize uint64 = uint64(unsafe.Sizeof(allocPreable{}))

// NewBufferAllocator created a new buffer allocator
func NewBufferAllocator(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64, pageSize uint16) (*BufferAllocator, error) {
	if bufSize&alignmentBytesMinusOne != 0 {
		return nil, ErrInvalidSize
	}

	buffer := &BufferAllocator{
		bufferPtr:  bufPtr,
		bufferSize: bufSize,
	}

	firstFree = alignSize(firstFree)

	buffer.header = (*header)(unsafe.Pointer(uintptr(bufPtr) + uintptr(firstFree)))
	buffer.SetPageSize(pageSize)

	if buffer.header.magic != magic {
		dataStart := alignSize(firstFree + uint64(unsafe.Sizeof(*buffer.header)))
		dataStart = buffer.GetPageOffset(dataStart + uint64(pageSize) - 1)

		buffer.header.magic = magic
		buffer.header.bufferStart = uint32(dataStart)
		buffer.header.dataWatermark = dataStart
		buffer.header.freePage = 0
		buffer.header.TotalUsed = 0
	}

	return buffer, nil
}

func (b *BufferAllocator) SetPageSize(s uint16) {
	b.header.pageSize = s
}

func (b *BufferAllocator) GetPageOffset(offset uint64) uint64 {
	psize := uint64(b.header.pageSize)
	return (offset / psize) * psize
}

func (b *BufferAllocator) SetBuffer(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64) {
	firstFree = alignSize(firstFree + uint64(uintptr(bufPtr)))

	b.bufferPtr = bufPtr
	b.bufferSize = bufSize
	b.header = (*header)(unsafe.Pointer(uintptr(firstFree)))
}

func (b *BufferAllocator) GetFree() uint64 {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.bufferSize - b.header.dataWatermark
}

func (b *BufferAllocator) GetUsed() uint64 {
	return atomic.LoadUint64(&b.header.TotalUsed)
}

func (b *BufferAllocator) GetCapacity() uint64 {
	return b.bufferSize
}

func (b *BufferAllocator) GetPtr(pos uint64) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(pos))
}

func (b *BufferAllocator) GetOffset(p unsafe.Pointer) uint64 {
	return uint64(uintptr(p) - uintptr(b.bufferPtr))
}

func (b *BufferAllocator) countSequensialFreePages(offset uint64) uint64 {
	psize := uint64(b.header.pageSize)

	for p := offset; ; p += psize {
		l := (*chunk)(b.GetPtr(p))
		l.nextFree = b.header.freePage
		b.header.freePage = p
	}
}

func (b *BufferAllocator) mergeChunks(offset uint64) uint64 {
	psize := uint64(b.header.pageSize)

	curOff := offset

	for curOff != 0 {
		curChunk := b.getChunk(curOff)

		if curOff+uint64(curChunk.size)*psize == b.header.dataWatermark {
			b.header.dataWatermark -= uint64(curChunk.size) * psize
			curOff = curChunk.nextFree
			continue
		}

		if curChunk.nextFree == 0 {
			break
		}

		nextChunk := b.getChunk(curChunk.nextFree)

		if curChunk.nextFree == curOff+uint64(curChunk.size)*psize {
			curChunk.nextFree = nextChunk.nextFree
			curChunk.size += nextChunk.size
			continue
		} else if curChunk.nextFree+uint64(nextChunk.size)*psize == curOff {
			nextChunk.size += curChunk.size
			curOff = curChunk.nextFree
			continue
		}

		break
	}

	return curOff
}

// Allocate a new buffer of specific size
func (b *BufferAllocator) Allocate(size uint64, zero bool) (uint64, error) {
	if size == 0 {
		return 0, ErrInvalidSize
	}

	// Ensure alignement
	size = alignSize(size)
	psize := uint64(b.header.pageSize)

	pagesNeeded := (size + psize - 1) / psize

	b.mux.Lock()

	var p uint64
	chunk := b.getChunk(b.header.freePage)
	if b.header.freePage != 0 && chunk.size == uint32(pagesNeeded) {
		p = b.header.freePage
		b.header.freePage = chunk.nextFree
		//println("allocate page", p, "new free", *l)
	} else if b.header.freePage != 0 && chunk.size > uint32(pagesNeeded) {
		p = b.header.freePage
		newChunk := b.getChunk(p + pagesNeeded*psize)
		newChunk.nextFree = chunk.nextFree
		newChunk.size = chunk.size - uint32(pagesNeeded)
		b.header.freePage = p + pagesNeeded*psize
	} else {
		if b.header.dataWatermark+pagesNeeded*psize > b.bufferSize {
			b.mux.Unlock()
			return 0, ErrOutOfMemory
		}

		p = b.header.dataWatermark
		b.header.dataWatermark += pagesNeeded * psize
	}

	b.mux.Unlock()

	if zero {
		buf := (*[maxBufferSize]byte)(b.GetPtr(p))[:size]
		for i := range buf { // Optimized by the compiler to simple memclr
			buf[i] = 0
		}
	}

	atomic.AddUint64(&b.header.TotalUsed, pagesNeeded*psize)

	// fmt.Printf("+ allocate %d bytes at %d\n", size, p)

	return p, nil
}

func (b *BufferAllocator) Deallocate(offset, size uint64) error {
	// Ensure alignement
	size = alignSize(size)
	psize := uint64(b.header.pageSize)

	if offset%psize != 0 {
		return fmt.Errorf("Free of non page aligned address %d (%d)", offset, offset%psize)
	}

	pagesNeeded := (size + psize - 1) / psize

	atomic.AddUint64(&b.header.TotalUsed, ^uint64(pagesNeeded*psize-1))

	// println("++ Freeing ", size, "at ", offset)

	b.mux.Lock()
	defer b.mux.Unlock()

	l := b.getChunk(offset)
	l.nextFree = b.header.freePage
	l.size = uint32(pagesNeeded)
	b.header.freePage = b.mergeChunks(offset)

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
	chunkPos := b.header.freePage
	var c *chunk
	i := 0
	s := uint64(0)
	fmt.Printf("---------------------------------------\n")
	for chunkPos != 0 {
		c = b.getChunk(chunkPos)

		fmt.Printf("Free chunk %d to %d (pages: %d)\n", chunkPos, uint32(chunkPos)+c.size*uint32(b.header.pageSize), c.size)

		chunkPos = c.nextFree
		i++
		s += uint64(c.size)
	}
	fmt.Printf("---------------------------------------\n")
	fmt.Printf("  Total free chunks: %d\n", i)
	fmt.Printf("  Total free pages : %d\n", s)

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
