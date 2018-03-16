package ebakusdb

import (
	"sync/atomic"
)

type Ptr uint64

func (p *Ptr) isNull() bool {
	return *p == 0
}

type RefCounted interface {
	Retain()
	Release() bool
	GetRefCount() int
}

type RefCountedObject struct {
	refCount int32
}

func (p *RefCountedObject) Retain() {
	atomic.AddInt32(&p.refCount, 1)
}

type ByteArray struct {
	Offset uint64
	Size   uint32
}

func (p *ByteArray) isNull() bool {
	return p.Offset == 0
}
