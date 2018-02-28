package ebakusdb

import (
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

func newNode(mm balloc.MemoryManager) (*Ptr, *Node, error) {
	size := uint64(unsafe.Sizeof(Node{}))
	offset, err := mm.Allocate(size, true)
	if err != nil {
		return nil, nil, err
	}
	p := Ptr(offset)
	n := p.getNode(mm)
	n.refCount = 1
	//*n = Node{RefCountedObject: RefCountedObject{refCount: 1}}

	//println("**NODE** Allocate", offset, size)

	return &p, n, nil
}

func (p *Ptr) getNode(mm balloc.MemoryManager) *Node {
	return (*Node)(mm.GetPtr(uint64(*p)))
}

func (nPtr *Ptr) NodeRelease(mm balloc.MemoryManager) bool {
	if *nPtr == 0 {
		return false
	}
	n := nPtr.getNode(mm)
	n.refCount--

	//fmt.Printf("Deref node %d with refs: %d\n", *nPtr, n.refCount)

	if n.refCount <= 0 {
		n.prefixPtr.Release(mm)
		n.keyPtr.Release(mm)
		n.valPtr.Release(mm)

		for _, ePtr := range n.edges {
			ePtr.NodeRelease(mm)
		}

		size := uint64(unsafe.Sizeof(Node{}))
		//println("**NODE**")
		if err := mm.Deallocate(uint64(*nPtr), size); err != nil {
			panic(err)
		}

		return true
	}

	return false
}
