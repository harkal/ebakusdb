package ebakusdb

import (
	"bytes"

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
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				b := db.getBytes(&n.valPtr)
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
		nprefix := db.getBytes(&n.prefixPtr)
		if bytes.HasPrefix(search, nprefix) {
			search = search[n.prefixPtr.Size:]
		} else {
			break
		}
	}
	return nil, false
}

func (n *Node) LongestPrefix(db *DB, k []byte) ([]byte, interface{}, bool) {
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

		prefix := db.getBytes(&n.prefixPtr)
		if bytes.HasPrefix(search, prefix) {
			search = search[len(prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return db.getBytes(&last.keyPtr), db.getBytes(&last.valPtr), true
	}
	return nil, nil, false
}

func (n *Node) Release() bool {
	return false
}

/*
func (n *Node) Release(db *DB) bool {
	n.refCount--

	if n.refCount <= 0 {
		if n.prefixPtr != nil {
			if err := db.allocator.Deallocate(n.prefixPtr.Offset); err != nil {
				panic(err)
			}
		}
		if n.leafPtr != nil {
			if err := db.allocator.Deallocate(n.leafPtr.Offset); err != nil {
				panic(err)
			}
		}
		return false
	}

	return false
}
*/

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
		ncp, err := t.db.cloneBytes(&n.prefixPtr)
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

func (t *Txn) insert(nodePtr *Ptr, k, search, v []byte) (*Ptr, *[]byte, bool) {
	n := t.db.getNode(nodePtr)
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal *[]byte
		didUpdate := false
		if n.isLeaf() {
			oldValSlice := t.db.getBytes(&n.valPtr)
			oldVal = &oldValSlice
			didUpdate = true
		}

		ncPtr := t.writeNode(nodePtr, true)
		nc := t.db.getNode(ncPtr)

		nc.keyPtr = *t.db.newBytesFromSlice(k)
		nc.valPtr = *t.db.newBytesFromSlice(v)

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

		nn.keyPtr = *t.db.newBytesFromSlice(k)
		nn.valPtr = *t.db.newBytesFromSlice(v)
		nn.prefixPtr = *t.db.newBytesFromSlice(search)

		nc := t.writeNode(nodePtr, false)
		t.db.getNode(nc).edges[edgeLabel] = *nnPtr
		return nc, nil, false
	}

	child := t.db.getNode(&childPtr)

	// Determine longest prefix of the search key on match
	childPrefix := t.db.getBytes(&child.prefixPtr)
	commonPrefix := longestPrefix(search, childPrefix)
	if commonPrefix == len(childPrefix) {
		search = search[commonPrefix:]
		newChildPtr, oldVal, didUpdate := t.insert(&childPtr, k, search, v)
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

	splitNode.prefixPtr = *t.db.newBytesFromSlice(search[:commonPrefix])

	nc := t.db.getNode(ncPtr)
	t.db.getNode(&nc.edges[search[0]]).Release()
	nc.edges[search[0]] = *splitNodePtr

	// Restore the existing child node
	modChildPtr := t.writeNode(&childPtr, false)
	modChild := t.db.getNode(modChildPtr)
	pref := t.db.getBytes(&modChild.prefixPtr)

	splitNode.edges[pref[commonPrefix]] = *modChildPtr

	modChild.prefixPtr = *t.db.newBytesFromSlice(pref[commonPrefix:])

	// If the new key is a subset, add to to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.keyPtr = *t.db.newBytesFromSlice(k)
		splitNode.valPtr = *t.db.newBytesFromSlice(v)
		return ncPtr, nil, false
	}

	enPtr, en, err := t.db.newNode()
	if err != nil {
		panic(err)
	}
	en.keyPtr = *t.db.newBytesFromSlice(k)
	en.valPtr = *t.db.newBytesFromSlice(v)
	en.prefixPtr = *t.db.newBytesFromSlice(search)

	splitNode.edges[search[0]] = *enPtr

	return ncPtr, nil, false
}

func (t *Txn) Insert(k, v []byte) (*[]byte, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.db.getNode(t.root).Release()
		t.root = newRoot
	}
	return oldVal, didUpdate
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
