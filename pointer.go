package ebakusdb

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
	refCount int
}

func (p *RefCountedObject) Retain() {
	p.refCount++
}

type ByteArray struct {
	Offset uint64
	Size   uint32
}

func (p *ByteArray) isNull() bool {
	return p.Offset == 0
}
