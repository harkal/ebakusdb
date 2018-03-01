package ebakusdb

import (
	"bytes"

	"github.com/harkal/ebakusdb/balloc"
	"github.com/hashicorp/golang-lru/simplelru"
)

type Node struct {
	RefCountedObject
	prefixPtr ByteArray
	edges     [16]Ptr // Nodes

	// leaf case
	keyPtr ByteArray
	valPtr ByteArray
}

func (n *Node) isLeaf() bool {
	return !n.keyPtr.isNull()
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
				return &b, true
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

func (n *Node) Iterator(mm balloc.MemoryManager) *Iterator {
	return &Iterator{node: n, mm: mm}
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
		if edgeNode.isNull() {
			continue
		}
		//fmt.Printf("Ref node %d with refs: %d\n", edgeNode, edgeNode.getNode(mm).refCount)
		edgeNode.getNode(mm).Retain()
	}

	t.writable.Add(*ncPtr, nil)

	return ncPtr
}

func (t *Txn) insert(nodePtr *Ptr, k, search []byte, vPtr ByteArray) (*Ptr, *ByteArray, bool) {
	mm := t.db.allocator
	n := nodePtr.getNode(mm)
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal *ByteArray
		didUpdate := false
		if n.isLeaf() {
			didUpdate = true

			oldVal = &n.valPtr
			oldVal.Retain(mm)
		}

		ncPtr := t.writeNode(nodePtr)
		nc := ncPtr.getNode(mm)

		nc.keyPtr = *newBytesFromSlice(mm, k)
		nc.valPtr = vPtr
		nc.valPtr.Retain(mm)

		return ncPtr, oldVal, didUpdate
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
		//nnPtr.getNode(mm).Retain()
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

	//nc.edges[search[0]].NodeRelease(mm)
	nc.edges[search[0]] = *splitNodePtr

	// Restore the existing child node
	modChildPtr := t.writeNode(&childPtr)
	modChild := modChildPtr.getNode(mm)
	pref := modChild.prefixPtr.getBytes(mm)

	splitNode.edges[pref[commonPrefix]] = *modChildPtr

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
		if edgeNode.isNull() {
			continue
		}
		//fmt.Printf("Ref node %d with refs: %d\n", edgeNode, edgeNode.getNode(mm).refCount)
		edgeNode.getNode(mm).Retain()
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

	// Copy this node. WATCH OUT - it's safe to pass "false" here because we
	// will only ADD a leaf via nc.mergeChild() if there isn't one due to
	// the !nc.isLeaf() check in the logic just below. This is pretty subtle,
	// so be careful if you change any of the logic here.
	ncPtr := t.writeNode(nPtr)
	nc := ncPtr.getNode(mm)

	// Delete the edge if the node has no edges
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

	oVal := oldVal.getBytes(mm)

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

// Get returns the key
func (t *Txn) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return t.root.getNode(t.db.allocator).Get(t.db, k)
}

func (db *DB) Commit(txn *Txn) error {
	txn.Commit()
	return nil
}

func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
