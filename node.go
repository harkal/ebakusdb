package ebakusdb

import (
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

func newNode(mm balloc.MemoryManager) (*Ptr, *Node, error) {
	size := uint64(unsafe.Sizeof(Node{}))
	offset, err := mm.Allocate(size)
	if err != nil {
		return nil, nil, err
	}
	p := &Ptr{Offset: offset, Size: size}
	n := p.getNode(mm)
	n.Retain()
	return p, n, nil
}

func (p *Ptr) getNode(mm balloc.MemoryManager) *Node {
	return (*Node)(mm.GetPtr(p.Offset))
}

func (nPtr *Ptr) NodeRelease(mm balloc.MemoryManager) bool {
	if nPtr.Offset == 0 {
		return false
	}
	n := nPtr.getNode(mm)
	n.refCount--

	//fmt.Printf("Deref node with refs: %d\n", n.refCount)

	if n.refCount <= 0 {
		n.prefixPtr.Release(mm)
		n.keyPtr.Release(mm)
		n.valPtr.Release(mm)

		for _, ePtr := range n.edges {
			ePtr.NodeRelease(mm)
		}

		mm.Deallocate(nPtr.Offset, nPtr.Size)
		return true
	}

	return false
}