package ebakusdb

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/ebakus/ebakusdb/balloc"
	"github.com/hashicorp/golang-lru/simplelru"
)

type Node struct {
	RefCountedObject
	prefixPtr ByteArray
	edges     [16]Ptr // Nodes

	// leaf case
	keyPtr ByteArray
	valPtr ByteArray

	nodePtr Ptr
}

var nodeCount int64

func GetNodeCount() int64 {
	return atomic.LoadInt64(&nodeCount)
}

func newNode(mm balloc.MemoryManager) (*Ptr, *Node, error) {
	size := uint64(unsafe.Sizeof(Node{}))
	offset, err := mm.Allocate(size, true)
	if err != nil {
		return nil, nil, err
	}
	p := Ptr(offset)
	n := p.getNode(mm)
	n.refCount = 1

	atomic.AddInt64(&nodeCount, 1)

	return &p, n, nil
}

func (p *Ptr) getNode(mm balloc.MemoryManager) *Node {
	return (*Node)(mm.GetPtr(uint64(*p)))
}

func (p *Ptr) NodeRetain(mm balloc.MemoryManager) bool {
	if *p == 0 {
		return false
	}
	p.getNode(mm).Retain()
	return true
}

func (p *Ptr) getNodeIterator(mm balloc.MemoryManager) *Iterator {
	return &Iterator{rootNode: *p, node: *p, mm: mm}
}

func (nPtr *Ptr) NodeRelease(mm balloc.MemoryManager) bool {
	if *nPtr == 0 {
		return false
	}
	n := nPtr.getNode(mm)

	if atomic.AddInt32(&n.refCount, -1) <= 0 {
		n.prefixPtr.Release(mm)
		n.keyPtr.Release(mm)
		n.valPtr.Release(mm)

		for _, ePtr := range n.edges {
			ePtr.NodeRelease(mm)
		}

		n.nodePtr.NodeRelease(mm)

		size := uint64(unsafe.Sizeof(Node{}))
		nodeCount--
		// println("**NODE** Release", *nPtr, nodeCount, mm.GetUsed())
		if err := mm.Deallocate(uint64(*nPtr), size); err != nil {
			panic(err)
		}

		return true
	}

	return false
}

func (n *Node) isLeaf() bool {
	return !n.keyPtr.isNull() || !n.nodePtr.isNull()
}

func (n *Node) hasOneChild() bool {
	count := 0
	for _, edgeNode := range n.edges {
		if !edgeNode.isNull() {
			count++
			if count > 1 {
				return false
			}
		}
	}
	return count == 1
}

func (n *Node) getFirstChild() Ptr {
	for _, edgeNodePtr := range n.edges {
		if !edgeNodePtr.isNull() {
			return edgeNodePtr
		}
	}
	return 0
}

func (n *Node) Get(db *DB, k []byte) (*[]byte, bool) {
	mm := db.allocator
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				b := n.valPtr.getBytes(mm)
				ob := make([]byte, len(b))
				copy(ob, b)
				return &ob, true
			}
			break
		}

		// Look for an edge
		nPtr := &n.edges[search[0]]
		if nPtr.isNull() {
			break
		}

		n = nPtr.getNode(mm)

		// Consume the search prefix
		nprefix := n.prefixPtr.getBytes(mm)
		if bytes.HasPrefix(search, nprefix) {
			search = search[n.prefixPtr.Size:]
		} else {
			break
		}
	}
	return nil, false
}

func (n *Node) LongestPrefix(db *DB, k []byte) ([]byte, interface{}, bool) {
	mm := db.allocator
	var last *Node
	search := k
	for {
		if n.isLeaf() {
			last = n
		}

		if len(search) == 0 {
			break
		}

		nPtr := n.edges[search[0]]
		if nPtr.isNull() {
			break
		}
		n = nPtr.getNode(mm)

		prefix := n.prefixPtr.getBytes(mm)
		if bytes.HasPrefix(search, prefix) {
			search = search[len(prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return last.keyPtr.getBytes(mm), last.valPtr.getBytes(mm), true
	}
	return nil, nil, false
}

func (n *Node) printTree(mm balloc.MemoryManager, child int, indent string, last bool) {
	fmt.Printf(indent)

	if last {
		fmt.Printf("\\-(%d)", child)
		indent += "  "
	} else {
		fmt.Printf("|-(%d)", child)
		indent += "| "
	}

	fmt.Printf("[%d] Prefix[%d]: (%s) Refs: %d ", mm.GetOffset(unsafe.Pointer(n)), n.prefixPtr, safeStringFromEncoded(n.prefixPtr.getBytes(mm)), n.refCount)

	if n.isLeaf() {
		fmt.Printf(" Key[%d]: (%s)[%d] Value[%d]: (%s)[%d] ",
			n.keyPtr,
			string(decodeKey(n.keyPtr.getBytes(mm))),
			*n.keyPtr.getBytesRefCount(mm),
			n.valPtr,
			string(n.valPtr.getBytes(mm)),
			*n.valPtr.getBytesRefCount(mm))
	}

	fmt.Println("")

	lastIndex := 16
	for i := 15; i >= 0; i-- {
		if !n.edges[i].isNull() {
			lastIndex = i
			break
		}
	}

	i := 0
	for _, edgeNodePtr := range n.edges {
		i++
		if edgeNodePtr.isNull() {
			continue
		}

		edgeNode := edgeNodePtr.getNode(mm)
		edgeNode.printTree(mm, i, indent, i-1 == lastIndex)
	}

	if !n.nodePtr.isNull() {
		edgeNode := n.nodePtr.getNode(mm)
		edgeNode.printTree(mm, -1, indent, true)
	}

	// fmt.Printf("%*s", ident, "")
	// fmt.Printf("Prefix: (%s) ", safeStringFromEncoded(n.prefixPtr.getBytes(mm)))
	// if n.isLeaf() {
	// 	fmt.Printf("%*s", ident, "")
	// 	fmt.Printf("Key: (%s) Value: (%s) ", string(decodeKey(n.keyPtr.getBytes(mm))), string(n.valPtr.getBytes(mm)))
	// }
	// fmt.Printf("Refs: %d\n", n.refCount)
	// for label, edgeNodePtr := range n.edges {
	// 	if edgeNodePtr.isNull() {
	// 		continue
	// 	}
	// 	fmt.Printf("%*s", ident+4, "")
	// 	fmt.Printf("Edge %d (%d):\n", label, edgeNodePtr)
	// 	edgeNode := edgeNodePtr.getNode(mm)
	// 	edgeNode.printTree(mm, ident+8)
	// }
}

const defaultWritableCache = 8192

type Txn struct {
	db       *DB
	root     Ptr
	snap     *Snapshot
	writable *simplelru.LRU
}

func (t *Txn) writeNode(nodePtr *Ptr) *Ptr {
	mm := t.db.allocator
	if t.writable == nil {
		lru, err := simplelru.NewLRU(defaultWritableCache, nil)
		if err != nil {
			panic(err)
		}
		t.writable = lru
	}

	n := nodePtr.getNode(mm)

	if _, ok := t.writable.Get(*nodePtr); ok {
		//println("hit", t.writable.Len())
		n.Retain()
		return nodePtr
	}

	//println("miss", t.writable.Len())

	ncPtr, nc, err := newNode(mm)
	if err != nil {
		panic(err)
	}

	nc.keyPtr = n.keyPtr
	nc.keyPtr.Retain(mm)
	nc.valPtr = n.valPtr
	nc.valPtr.Retain(mm)
	nc.prefixPtr = n.prefixPtr
	nc.prefixPtr.Retain(mm)

	nc.edges = n.edges

	for _, edgeNode := range nc.edges {
		edgeNode.NodeRetain(mm)
	}

	if !n.nodePtr.isNull() {
		nc.nodePtr = n.nodePtr
		nc.nodePtr.NodeRetain(mm)
	}

	t.writable.Add(*ncPtr, nil)

	return ncPtr
}

func (t *Txn) insert(nodePtr *Ptr, k, search []byte, vPtr ByteArray) (*Ptr, *ByteArray, bool) {
	if err := vPtr.checkBytesLength(); err != nil {
		return nil, nil, false
	}

	mm := t.db.allocator
	n := nodePtr.getNode(mm)
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal ByteArray
		didUpdate := false
		if n.isLeaf() {
			didUpdate = true

			oldVal = n.valPtr
			oldVal.Retain(mm)
		}

		ncPtr := t.writeNode(nodePtr)
		nc := ncPtr.getNode(mm)

		nc.keyPtr = *newBytesFromSlice(mm, k)
		nc.valPtr = vPtr
		nc.valPtr.Retain(mm)

		return ncPtr, &oldVal, didUpdate
	}

	edgeLabel := search[0]
	childPtr := n.edges[edgeLabel]

	// No edge, create one
	if childPtr.isNull() {
		nnPtr, nn, err := newNode(mm)
		if err != nil {
			panic(err)
		}

		nn.keyPtr = *newBytesFromSlice(mm, k)
		nn.valPtr = vPtr
		nn.valPtr.Retain(mm)
		nn.prefixPtr = *newBytesFromSlice(mm, search)

		nc := t.writeNode(nodePtr)
		nc.getNode(mm).edges[edgeLabel] = *nnPtr
		return nc, nil, false
	}

	child := childPtr.getNode(mm)

	// Determine longest prefix of the search key on match
	childPrefix := child.prefixPtr.getBytes(mm)
	commonPrefix := longestPrefix(search, childPrefix)
	if commonPrefix == len(childPrefix) {
		search = search[commonPrefix:]
		newChildPtr, oldVal, didUpdate := t.insert(&childPtr, k, search, vPtr)
		if newChildPtr != nil {
			ncPtr := t.writeNode(nodePtr)
			nc := ncPtr.getNode(mm)
			nc.edges[edgeLabel].NodeRelease(mm)
			nc.edges[edgeLabel] = *newChildPtr
			return ncPtr, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// Split the node
	ncPtr := t.writeNode(nodePtr)
	nc := ncPtr.getNode(mm)

	splitNodePtr, splitNode, err := newNode(mm)
	if err != nil {
		panic(err)
	}

	splitNode.prefixPtr = *newBytesFromSlice(mm, search[:commonPrefix])

	// Restore the existing child node
	modChildPtr := t.writeNode(&childPtr)
	modChild := modChildPtr.getNode(mm)
	pref := modChild.prefixPtr.getBytes(mm)

	splitNode.edges[pref[commonPrefix]] = *modChildPtr

	nc.edges[edgeLabel].NodeRelease(mm)
	nc.edges[edgeLabel] = *splitNodePtr

	modChild.prefixPtr = *newBytesFromSlice(mm, pref[commonPrefix:])

	// If the new key is a subset, add to to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.keyPtr = *newBytesFromSlice(mm, k)
		splitNode.valPtr = vPtr
		vPtr.Retain(mm)
		return ncPtr, nil, false
	}

	enPtr, en, err := newNode(mm)
	if err != nil {
		panic(err)
	}
	en.keyPtr = *newBytesFromSlice(mm, k)
	en.valPtr = vPtr
	vPtr.Retain(mm)
	en.prefixPtr = *newBytesFromSlice(mm, search)

	splitNode.edges[search[0]] = *enPtr

	return ncPtr, nil, false
}

func (t *Txn) mergeChild(n *Node) {
	mm := t.db.allocator

	childPtr := n.getFirstChild()
	child := childPtr.getNode(mm)

	// Merge the nodes.
	mergedPrefix := concat(n.prefixPtr.getBytes(mm), child.prefixPtr.getBytes(mm))
	n.prefixPtr.Release(mm)
	n.prefixPtr = *newBytesFromSlice(mm, mergedPrefix)
	n.keyPtr.Release(mm) // check if needed
	n.valPtr.Release(mm) // check if needed
	n.keyPtr = child.keyPtr
	n.keyPtr.Retain(mm)
	n.valPtr = child.valPtr
	n.valPtr.Retain(mm)

	n.edges = child.edges

	for _, edgeNode := range n.edges {
		edgeNode.NodeRetain(mm)
	}

	childPtr.NodeRelease(mm)
}

func (t *Txn) delete(parentPtr, nPtr *Ptr, search []byte) (node *Ptr) {
	mm := t.db.allocator
	n := nPtr.getNode(mm)

	// Check for key exhaustion
	if len(search) == 0 {
		if !n.isLeaf() {
			return nil
		}

		// Remove the leaf node
		ncPtr := t.writeNode(nPtr)
		nc := ncPtr.getNode(mm)
		nc.keyPtr.Release(mm)
		nc.valPtr.Release(mm)

		// Check if this node should be merged
		if *nPtr != t.root && nc.hasOneChild() {
			t.mergeChild(nc)
		}

		return ncPtr
	}

	edgeLabel := search[0]
	childPtr := n.edges[edgeLabel]
	if childPtr.isNull() {
		return nil
	}

	child := childPtr.getNode(mm)
	childPrefix := child.prefixPtr.getBytes(mm)

	if !bytes.HasPrefix(search, childPrefix) {
		return nil
	}

	// Consume the search prefix
	search = search[len(childPrefix):]
	newChildPtr := t.delete(nPtr, &childPtr, search)
	if newChildPtr == nil {
		return nil
	}

	newChild := newChildPtr.getNode(mm)

	ncPtr := t.writeNode(nPtr)
	nc := ncPtr.getNode(mm)

	nc.edges[edgeLabel].NodeRelease(mm)
	if newChild.isLeaf() == false && newChild.getFirstChild() == 0 {
		nc.edges[edgeLabel] = 0
		if *nPtr != t.root && nc.hasOneChild() && !nc.isLeaf() {
			t.mergeChild(nc)
		}
		newChildPtr.NodeRelease(mm)
	} else {
		nc.edges[edgeLabel] = *newChildPtr
	}

	return ncPtr
}

func (t *Txn) Insert(k, v []byte) (*[]byte, bool) {
	if err := checkBytesLength(v); err != nil {
		return nil, false
	}

	mm := t.db.allocator
	k = encodeKey(k)
	vPtr := *newBytesFromSlice(mm, v)
	newRoot, oldVal, didUpdate := t.insert(&t.root, k, k, vPtr)
	vPtr.Release(mm)
	if newRoot != nil {
		t.root.NodeRelease(mm)
		t.root = *newRoot
	}

	if oldVal == nil {
		return nil, didUpdate
	}

	val := oldVal.getBytes(mm)
	oVal := make([]byte, len(val))
	copy(oVal, val)
	oldVal.Release(mm)

	return &oVal, didUpdate
}

func (t *Txn) Delete(k []byte) bool {
	mm := t.db.allocator
	k = encodeKey(k)
	newRoot := t.delete(nil, &t.root, k)
	if newRoot != nil {
		t.root.NodeRelease(mm)
		t.root = *newRoot
		return true
	}
	return false
}

func (t *Txn) Commit() (uint64, error) {
	t.writable = nil
	t.db.Grow()
	if t.snap != nil {
		t.snap.root.NodeRelease(t.db.allocator)
		t.snap.root = *t.Root()
	} else {
		h := t.db.header
		h.root.NodeRelease(t.db.allocator)
		h.root = *t.Root()
	}
	return uint64(*t.Root()), nil
}

func (t *Txn) Rollback() {
	t.writable = nil
	t.root.NodeRelease(t.db.allocator)
}

func (t *Txn) Root() *Ptr {
	return &t.root
}

func (t *Txn) RootNode() *Node {
	return t.Root().getNode(t.db.allocator)
}

func (t *Txn) printTree() {
	t.RootNode().printTree(t.db.allocator, 0, "", false)
}

// Get returns the key
func (t *Txn) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return t.root.getNode(t.db.allocator).Get(t.db, k)
}

func (db *DB) Commit(txn *Txn) error {
	txn.Commit()
	return nil
}
