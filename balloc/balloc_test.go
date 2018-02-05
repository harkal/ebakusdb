package balloc_test

import (
	"testing"
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

func Test_CreateBuffer(t *testing.T) {
	buffer := make([]byte, 1024*1024) // 1MB

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	buffer2 := make([]byte, 1024*1024+1)
	ba, err = balloc.NewBufferAllocator(unsafe.Pointer(&buffer2[0]), uint64(len(buffer2)), 0)
	if err != balloc.ErrInvalidSize {
		t.Fatal("Should not accept unaligned size")
	}
}

func Test_Allocate(t *testing.T) {
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	totalSpace = ba.GetFree()

	_, err = ba.Allocate(1024)
	if err != nil {
		t.Fatal("failed to allocate 1024 bytes")
	}

	_, err = ba.Allocate(totalSpace - 100)
	if err != balloc.ErrOutOfMemory {
		t.Fatal("Unexpected error allocating totalSpace - 100 bytes")
	}

	_, err = ba.Allocate(totalSpace - 2048)
	if err != nil {
		t.Fatal("Failed allocating totalSpace - 1024 bytes")
	}
}

func Test_AllocateGrow(t *testing.T) {
	totalSpace := uint64(1024 + 48) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	_, err = ba.Allocate(1024)
	if err != nil {
		t.Fatal("failed to allocate 1024 bytes")
	}

	_, err = ba.Allocate(200)
	if err != balloc.ErrOutOfMemory {
		t.Fatal("Unexpected error allocating 200 bytes")
	}

	buffer2 := make([]byte, totalSpace*2)
	copy(buffer2, buffer)
	ba.SetBuffer(unsafe.Pointer(&buffer2[0]), uint64(len(buffer2)), 0)

	_, err = ba.Allocate(10)
	if err != nil {
		t.Fatal("failed to allocate 200 bytes")
	}

	_, err = ba.Allocate(800)
	if err != nil {
		t.Fatal("failed to allocate 200 bytes")
	}

	_, err = ba.Allocate(24)
	if err != nil {
		t.Fatal("failed to allocate 200 bytes")
	}
}

func Test_Alignment(t *testing.T) {
	alignmentMask := uint64(8 - 1)
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	p1, err := ba.Allocate(16)
	if err != nil {
		t.Fatal("failed to allocate 10 bytes")
	}

	if p1&alignmentMask != 0 {
		t.Fatalf("Allocated buffer not aligned: (%d) %b", p1, p1)
	}

	p2, err := ba.Allocate(8)
	if err != nil {
		t.Fatal("failed to allocate 8 bytes")
	}

	if p2&alignmentMask != 0 {
		t.Fatalf("Allocated buffer not aligned: (%d) %b", p2, p2)
	}

	p3, err := ba.Allocate(145)
	if err != nil {
		t.Fatal("failed to allocate 8 bytes")
	}

	if p2&alignmentMask != 0 {
		t.Fatalf("Allocated buffer not aligned: (%d) %b", p3, p3)
	}

}

func Test_DeallocateAligned(t *testing.T) {
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	free := ba.GetFree()
	p1, err := ba.Allocate(16)
	if err != nil {
		t.Fatal("failed to allocate 10 bytes")
	}
	ba.Deallocate(p1, 16)
	if ba.GetFree() < free {
		t.Fatal("Incorrect free space")
	}
}

func Test_DeallocateMissaligned(t *testing.T) {
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(unsafe.Pointer(&buffer[0]), uint64(len(buffer)), 0)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	free := ba.GetFree()
	p1, err := ba.Allocate(15)
	if err != nil {
		t.Fatal("failed to allocate 10 bytes")
	}
	ba.Deallocate(p1, 15)
	if ba.GetFree() < free {
		t.Fatal("Incorrect free space")
	}
}
