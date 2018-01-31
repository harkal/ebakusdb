package ebakusdb

import (
	"bytes"
	"sort"

	"github.com/hashicorp/golang-lru/simplelru"
)

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *Ptr
}

type edges []edge

func (e edges) Len() int {
	return len(e)
}

func (e edges) Less(i, j int) bool {
	return e[i].label < e[j].label
}

func (e edges) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e edges) Sort() {
	sort.Sort(e)
}

type leafNode struct {
	keyPtr ByteArray
	val    interface{}
}

type Node struct {
	RefCountedObject
	leafPtr   *Ptr
	prefixPtr *ByteArray
	edges     edges
}

func (n *Node) isLeaf() bool {
	return n.leafPtr != nil
}

func (n *Node) Get(db *DB, k []byte) (interface{}, bool) {
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				return db.getLeafNode(n.leafPtr).val, true
			}
			break
		}

		// Look for an edge
		_, nPtr := n.getEdge(search[0])
		if nPtr == nil {
			break
		}

		n = db.getNode(nPtr)

		// Consume the search prefix
		nprefix := db.getBytes(n.prefixPtr)
		if bytes.HasPrefix(search, nprefix) {
			search = search[n.prefixPtr.Size:]
		} else {
			break
		}
	}
	return nil, false
}

func (n *Node) addEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte) (int, *Ptr) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) delEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node) LongestPrefix(db *DB, k []byte) ([]byte, interface{}, bool) {
	var last *Ptr
	search := k
	for {
		if n.isLeaf() {
			last = n.leafPtr
		}

		if len(search) == 0 {
			break
		}

		_, nPtr := n.getEdge(search[0])
		if nPtr == nil {
			break
		}

		prefix := db.getBytes(n.prefixPtr)
		if bytes.HasPrefix(search, prefix) {
			search = search[len(prefix):]
		} else {
			break
		}
	}
	if last != nil {
		l := db.getLeafNode(last)
		return db.getBytes(&l.keyPtr), l.val, true
	}
	return nil, nil, false
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

	nc.leafPtr = n.leafPtr

	if n.prefixPtr != nil {
		ncp, err := t.db.cloneBytes(n.prefixPtr)
		if err != nil {
			panic(err)
		}
		nc.prefixPtr = ncp
	}
	if len(n.edges) != 0 {
		nc.edges = make([]edge, len(n.edges))
		copy(nc.edges, n.edges)
	}

	t.writable.Add(ncPtr, nil)
	return ncPtr
}

func (t *Txn) insert(nodePtr *Ptr, k, search []byte, v interface{}) (*Ptr, interface{}, bool) {
	n := t.db.getNode(nodePtr)
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal interface{}
		didUpdate := false
		if n.isLeaf() {
			leaf := t.db.getLeafNode(n.leafPtr)
			oldVal = leaf.val
			didUpdate = true
		}

		ncPtr := t.writeNode(nodePtr, true)
		nc := t.db.getNode(ncPtr)

		leafPtr, leaf, err := t.db.newLeafNode()
		if err != nil {
			panic(err)
		}

		leaf.keyPtr = *t.db.newBytesFromSlice(k)
		leaf.val = v

		nc.leafPtr = leafPtr
		return ncPtr, oldVal, didUpdate
	}

	// Look for the edge
	idx, childPtr := n.getEdge(search[0])

	// No edge, create one
	if childPtr == nil {
		nnPtr, nn, err := t.db.newNode()
		if err != nil {
			panic(err)
		}

		leafPtr, leaf, err := t.db.newLeafNode()
		if err != nil {
			panic(err)
		}

		leaf.keyPtr = *t.db.newBytesFromSlice(k)
		leaf.val = v

		nn.leafPtr = leafPtr

		nn.prefixPtr = t.db.newBytesFromSlice(search)

		e := edge{
			label: search[0],
			node:  nnPtr,
		}
		nc := t.writeNode(nodePtr, false)
		t.db.getNode(nc).addEdge(e)
		return nc, nil, false
	}

	child := t.db.getNode(childPtr)

	// Determine longest prefix of the search key on match
	childPrefix := t.db.getBytes(child.prefixPtr)
	commonPrefix := longestPrefix(search, childPrefix)
	if commonPrefix == len(childPrefix) {
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(childPtr, k, search, v)
		if newChild != nil {
			ncPtr := t.writeNode(nodePtr, false)
			nc := t.db.getNode(ncPtr)
			nc.edges[idx].node = newChild
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

	splitNode.prefixPtr = t.db.newBytesFromSlice(search[:commonPrefix])

	nc := t.db.getNode(ncPtr)
	nc.replaceEdge(edge{
		label: search[0],
		node:  splitNodePtr,
	})

	// Restore the existing child node
	modChildPtr := t.writeNode(childPtr, false)
	modChild := t.db.getNode(modChildPtr)
	pref := t.db.getBytes(modChild.prefixPtr)
	splitNode.addEdge(edge{
		label: pref[commonPrefix],
		node:  modChildPtr,
	})
	modChild.prefixPtr = t.db.newBytesFromSlice(pref[commonPrefix:])

	leafPtr, leaf, err := t.db.newLeafNode()
	if err != nil {
		panic(err)
	}
	leaf.keyPtr = *t.db.newBytesFromSlice(k)
	leaf.val = v

	// If the new key is a subset, add to to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leafPtr = leafPtr
		return ncPtr, nil, false
	}

	enPtr, en, err := t.db.newNode()
	if err != nil {
		panic(err)
	}
	en.leafPtr = leafPtr
	en.prefixPtr = t.db.newBytesFromSlice(search)

	// Create a new edge for the node
	splitNode.addEdge(edge{
		label: search[0],
		node:  enPtr,
	})
	return ncPtr, nil, false
}

func (t *Txn) Insert(k []byte, v interface{}) (interface{}, bool) {
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
func (t *Txn) Get(k []byte) (interface{}, bool) {
	return t.db.getNode(t.root).Get(t.db, k)
}
