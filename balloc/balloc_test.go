package balloc_test

import (
	"testing"

	"github.com/harkal/ebakusdb/balloc"
)

func Test_CreateBuffer(t *testing.T) {
	buffer := make([]byte, 1024*1024) // 1MB

	ba, err := balloc.NewBufferAllocator(buffer)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	buffer2 := make([]byte, 1024*1024+1)
	ba, err = balloc.NewBufferAllocator(buffer2)
	if err != balloc.ErrInvalidSize {
		t.Fatal("Should not accept unaligned size")
	}
}

func Test_Allocate(t *testing.T) {
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(buffer)
	if err != nil || ba == nil {
		t.Fatal("failed to create buffer")
	}

	_, err = ba.Allocate(1024)
	if err != nil {
		t.Fatal("failed to allocate 1024 bytes")
	}

	_, err = ba.Allocate(totalSpace - 100)
	if err != balloc.ErrOutOfMemory {
		t.Fatal("Unexpected error allocating totalSpace - 100 bytes")
	}

	_, err = ba.Allocate(totalSpace - 1024)
	if err != nil {
		t.Fatal("Failed allocating totalSpace - 1024 bytes")
	}
}

func Test_Alignment(t *testing.T) {
	alignmentMask := uint64(8 - 1)
	totalSpace := uint64(1024 * 1024) // 1MB
	buffer := make([]byte, totalSpace)

	ba, err := balloc.NewBufferAllocator(buffer)
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
