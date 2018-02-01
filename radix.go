package ebakusdb

import (
	"bytes"

	"github.com/harkal/ebakusdb/balloc"
	"github.com/hashicorp/golang-lru/simplelru"
)

type Node struct {
	RefCountedObject
	prefixPtr ByteArray
	edges     [256]Ptr // Nodes

	// leaf case
	keyPtr ByteArray
	valPtr ByteArray
}

func (n *Node) isLeaf() bool {
	return !n.keyPtr.isNull()
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

		n = db.getNode(nPtr)

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
		n = db.getNode(&nPtr)

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

func (n *Node) Release(mm balloc.MemoryManager) bool {
	n.refCount--

	if n.refCount <= 0 {
		if !n.prefixPtr.isNull() {
			if err := mm.Deallocate(n.prefixPtr.Offset, n.prefixPtr.Size); err != nil {
				panic(err)
			}
		}
		if !n.keyPtr.isNull() {
			if err := mm.Deallocate(n.keyPtr.Offset, n.keyPtr.Size); err != nil {
				panic(err)
			}
		}
		if !n.valPtr.isNull() {
			if err := mm.Deallocate(n.keyPtr.Offset, n.keyPtr.Size); err != nil {
				panic(err)
			}
		}
		return false
	}

	return false
}

const defaultWritableCache = 8192

type Txn struct {
	db       *DB
	root     *Ptr
	writable *simplelru.LRU
}

func (t *Txn) writeNode(nodePtr *Ptr, forLeafUpdate bool) *Ptr {
	if t.writable == nil {
		lru, err := simplelru.NewLRU(defaultWritableCache, nil)
		if err != nil {
			panic(err)
		}
		t.writable = lru
	}

	n := t.db.getNode(nodePtr)

	if _, ok := t.writable.Get(*nodePtr); ok {
		n.Retain()
		return nodePtr
	}

	ncPtr, nc, err := t.db.newNode()
	if err != nil {
		panic(err)
	}

	nc.keyPtr = n.keyPtr
	nc.valPtr = n.valPtr

	if !n.prefixPtr.isNull() {
		ncp, err := n.prefixPtr.cloneBytes(t.db.allocator)
		if err != nil {
			panic(err)
		}
		nc.prefixPtr = *ncp
	}

	nc.edges = n.edges

	for _, edgeNode := range nc.edges {
		if edgeNode.isNull() {
			continue
		}
		t.db.getNode(&edgeNode).Retain()
	}

	t.writable.Add(ncPtr, nil)
	return ncPtr
}

func (t *Txn) insert(nodePtr *Ptr, k, search []byte, vPtr ByteArray) (*Ptr, *ByteArray, bool) {
	mm := t.db.allocator
	n := t.db.getNode(nodePtr)
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal *ByteArray
		didUpdate := false
		if n.isLeaf() {
			didUpdate = true

			oldVal = &n.valPtr
			oldVal.BytesRetain(mm)
		}

		ncPtr := t.writeNode(nodePtr, true)
		nc := t.db.getNode(ncPtr)

		nc.keyPtr = *newBytesFromSlice(mm, k)
		nc.valPtr = vPtr
		nc.valPtr.BytesRetain(mm)

		return ncPtr, oldVal, didUpdate
	}

	edgeLabel := search[0]
	childPtr := n.edges[edgeLabel]

	// No edge, create one
	if childPtr.isNull() {
		nnPtr, nn, err := t.db.newNode()
		if err != nil {
			panic(err)
		}

		nn.keyPtr = *newBytesFromSlice(mm, k)
		nn.valPtr = vPtr
		nn.valPtr.BytesRetain(mm)
		nn.prefixPtr = *newBytesFromSlice(mm, search)

		nc := t.writeNode(nodePtr, false)
		t.db.getNode(nc).edges[edgeLabel] = *nnPtr
		return nc, nil, false
	}

	child := t.db.getNode(&childPtr)

	// Determine longest prefix of the search key on match
	childPrefix := child.prefixPtr.getBytes(mm)
	commonPrefix := longestPrefix(search, childPrefix)
	if commonPrefix == len(childPrefix) {
		search = search[commonPrefix:]
		newChildPtr, oldVal, didUpdate := t.insert(&childPtr, k, search, vPtr)
		if newChildPtr != nil {
			ncPtr := t.writeNode(nodePtr, false)
			nc := t.db.getNode(ncPtr)
			nc.edges[edgeLabel] = *newChildPtr
			return ncPtr, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// Split the node
	ncPtr := t.writeNode(nodePtr, false)

	splitNodePtr, splitNode, err := t.db.newNode()
	if err != nil {
		panic(err)
	}

	splitNode.prefixPtr = *newBytesFromSlice(mm, search[:commonPrefix])

	nc := t.db.getNode(ncPtr)
	t.db.getNode(&nc.edges[search[0]]).Release(mm)
	nc.edges[search[0]] = *splitNodePtr

	// Restore the existing child node
	modChildPtr := t.writeNode(&childPtr, false)
	modChild := t.db.getNode(modChildPtr)
	pref := modChild.prefixPtr.getBytes(mm)

	t.db.getNode(&splitNode.edges[pref[commonPrefix]]).Release(mm)
	splitNode.edges[pref[commonPrefix]] = *modChildPtr

	modChild.prefixPtr = *newBytesFromSlice(mm, pref[commonPrefix:])

	// If the new key is a subset, add to to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.keyPtr = *newBytesFromSlice(mm, k)
		splitNode.valPtr = vPtr
		vPtr.BytesRetain(mm)
		return ncPtr, nil, false
	}

	enPtr, en, err := t.db.newNode()
	if err != nil {
		panic(err)
	}
	en.keyPtr = *newBytesFromSlice(mm, k)
	en.valPtr = vPtr
	vPtr.BytesRetain(mm)
	en.prefixPtr = *newBytesFromSlice(mm, search)

	t.db.getNode(&splitNode.edges[search[0]]).Release(mm)
	splitNode.edges[search[0]] = *enPtr

	return ncPtr, nil, false
}

func (t *Txn) Insert(k, v []byte) (*[]byte, bool) {
	mm := t.db.allocator
	vPtr := *newBytesFromSlice(mm, v)
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, vPtr)
	vPtr.BytesRelease(mm)
	if newRoot != nil {
		t.db.getNode(t.root).Release(mm)
		t.root = newRoot
	}

	if oldVal == nil {
		return nil, didUpdate

	}

	oVal := oldVal.getBytes(mm)
	return &oVal, didUpdate
}

func (t *Txn) Commit() *Ptr {
	t.writable = nil
	return t.root
}

func (t *Txn) Root() *Ptr {
	return t.root
}

// Get returns the key
func (t *Txn) Get(k []byte) (*[]byte, bool) {
	return t.db.getNode(t.root).Get(t.db, k)
}
