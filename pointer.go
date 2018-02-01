package ebakusdb

type Ptr struct {
	Offset uint64
	Size   uint64
}

func (p *Ptr) isNull() bool {
	return p.Offset == 0
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

func (p *RefCountedObject) Release() bool {
	p.refCount--
	return p.refCount == 0
}

func (p *RefCountedObject) GetRefCount() int {
	return p.refCount
}

type ByteArray struct {
	Offset uint64
	Size   uint64
}

func (p *ByteArray) isNull() bool {
	return p.Offset == 0
}
