package balloc

import (
	"errors"
	"fmt"
	"runtime"
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

	Lock()
	Unlock()
}

// BufferAllocator allocates memory in a preallocated buffer
type BufferAllocator struct {
	bufferPtr  unsafe.Pointer
	bufferSize uint64
	header     *header
	headerLock uintptr

	bufferMux sync.RWMutex
}

const magic uint32 = 0xca01af01

type header struct {
	magic         uint32
	BufferStart   uint32
	PageSize      uint16
	DataWatermark uint64
	FreePage      uint64
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
		buffer.header.BufferStart = uint32(dataStart)
		buffer.header.DataWatermark = dataStart
		buffer.header.FreePage = 0
		buffer.header.TotalUsed = 0
	}

	return buffer, nil
}

func (b *BufferAllocator) GetHeader() *header {
	return b.header
}

func (b *BufferAllocator) SetPageSize(s uint16) {
	b.header.PageSize = s
}

func (b *BufferAllocator) GetPageOffset(offset uint64) uint64 {
	psize := uint64(b.header.PageSize)
	return (offset / psize) * psize
}

func (b *BufferAllocator) Lock() {
	b.bufferMux.RLock()
}

func (b *BufferAllocator) Unlock() {
	b.bufferMux.RUnlock()
}

func (b *BufferAllocator) WLock() {
	b.bufferMux.Lock()
}

func (b *BufferAllocator) WUnlock() {
	b.bufferMux.Unlock()
}

func (b *BufferAllocator) headLock() {
	for !atomic.CompareAndSwapUintptr(&b.headerLock, 0, 1) {
		runtime.Gosched()
	}
}

func (b *BufferAllocator) headUnlock() {
	atomic.StoreUintptr(&b.headerLock, 0)
}

func (b *BufferAllocator) SetBuffer(bufPtr unsafe.Pointer, bufSize uint64, firstFree uint64) {
	firstFree = alignSize(firstFree + uint64(uintptr(bufPtr)))

	b.bufferPtr = bufPtr
	b.bufferSize = bufSize
	b.header = (*header)(unsafe.Pointer(uintptr(firstFree)))
}

func (b *BufferAllocator) GetFree() uint64 {
	b.headLock()
	defer b.headUnlock()
	return b.bufferSize - b.header.DataWatermark
}

func (b *BufferAllocator) GetUsed() uint64 {
	return atomic.LoadUint64(&b.header.TotalUsed)
}

func (b *BufferAllocator) GetCapacity() uint64 {
	return b.bufferSize
}

func (b *BufferAllocator) GetPtr(pos uint64) unsafe.Pointer {
	ret := unsafe.Pointer(uintptr(b.bufferPtr) + uintptr(pos))
	return ret
}

func (b *BufferAllocator) GetOffset(p unsafe.Pointer) uint64 {
	return uint64(uintptr(p) - uintptr(b.bufferPtr))
}

func (b *BufferAllocator) countSequensialFreePages(offset uint64) uint64 {
	psize := uint64(b.header.PageSize)

	for p := offset; ; p += psize {
		l := (*chunk)(b.GetPtr(p))
		l.nextFree = b.header.FreePage
		b.header.FreePage = p
	}
}

func (b *BufferAllocator) mergeChunks(offset uint64) uint64 {
	psize := uint64(b.header.PageSize)

	curOff := offset

	for curOff != 0 {
		curChunk := b.getChunk(curOff)

		if curOff+uint64(curChunk.size)*psize == b.header.DataWatermark {
			b.header.DataWatermark -= uint64(curChunk.size) * psize
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
	psize := uint64(b.header.PageSize)

	pagesNeeded := (size + psize - 1) / psize

	b.headLock()

	var p uint64
	chunk := b.getChunk(b.header.FreePage)
	if b.header.FreePage != 0 && chunk.size == uint32(pagesNeeded) {
		p = b.header.FreePage
		b.header.FreePage = chunk.nextFree
		//println("allocate page", p, "new free", *l)
	} else if b.header.FreePage != 0 && chunk.size > uint32(pagesNeeded) {
		p = b.header.FreePage
		newChunk := b.getChunk(p + pagesNeeded*psize)
		newChunk.nextFree = chunk.nextFree
		newChunk.size = chunk.size - uint32(pagesNeeded)
		b.header.FreePage = p + pagesNeeded*psize
	} else {
		if b.header.DataWatermark+pagesNeeded*psize > b.bufferSize {
			b.headUnlock()
			return 0, ErrOutOfMemory
		}

		p = b.header.DataWatermark
		b.header.DataWatermark += pagesNeeded * psize
	}

	b.headUnlock()

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
	psize := uint64(b.header.PageSize)

	if offset%psize != 0 {
		return fmt.Errorf("Free of non page aligned address %d (%d)", offset, offset%psize)
	}

	pagesNeeded := (size + psize - 1) / psize

	atomic.AddUint64(&b.header.TotalUsed, ^uint64(pagesNeeded*psize-1))

	// println("++ Freeing ", size, "at ", offset)

	b.headLock()

	l := b.getChunk(offset)
	l.nextFree = b.header.FreePage
	l.size = uint32(pagesNeeded)
	b.header.FreePage = b.mergeChunks(offset)

	b.headUnlock()

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
	chunkPos := b.header.FreePage
	var c *chunk
	i := 0
	s := uint64(0)
	fmt.Printf("---------------------------------------\n")
	for chunkPos != 0 {
		c = b.getChunk(chunkPos)

		fmt.Printf("Free chunk %d to %d (pages: %d)\n", chunkPos, uint32(chunkPos)+c.size*uint32(b.header.PageSize), c.size)

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
