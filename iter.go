package ebakusdb

import (
	"bytes"

	"github.com/harkal/ebakusdb/balloc"
)

type edge struct {
	key  byte
	node *Node
}

type edges []edge

type Iterator struct {
	node  *Node
	stack []edges
	mm    balloc.MemoryManager
}

func (i *Iterator) SeekPrefix(prefix []byte) {
	prefix = encodeKey(prefix)
	i.stack = nil
	n := i.node
	search := prefix
	for {
		if len(search) == 0 {
			i.node = n
			return
		}

		nPtr := n.edges[search[0]]
		if nPtr.isNull() {
			i.node = nil
			return
		}
		n = nPtr.getNode(i.mm)

		nprefix := n.prefixPtr.getBytes(i.mm)
		if bytes.HasPrefix(search, nprefix) {
			search = search[len(nprefix):]

		} else if bytes.HasPrefix(nprefix, search) {
			i.node = n
			return
		} else {
			i.node = nil
			return
		}
	}
}

func (i *Iterator) Next() ([]byte, []byte, bool) {
	if i.stack == nil && i.node != nil {
		i.stack = []edges{
			edges{
				edge{node: i.node},
			},
		}
	}

	for len(i.stack) > 0 {
		n := len(i.stack)
		last := i.stack[n-1]
		elem := last[0].node

		if len(last) > 1 {
			i.stack[n-1] = last[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		es := make(edges, 0)
		for k, nPtr := range elem.edges {
			if !nPtr.isNull() {
				e := edge{key: byte(k), node: nPtr.getNode(i.mm)}
				es = append(es, e)
			}
		}

		if len(es) > 0 {
			i.stack = append(i.stack, es)
		}

		if elem.isLeaf() {
			return decodeKey(elem.keyPtr.getBytes(i.mm)), elem.valPtr.getBytes(i.mm), true
		}
	}

	return nil, nil, false
}

type ResultIterator struct {
	db   *DB
	iter *Iterator

	tableRoot *Node
}

func (ri *ResultIterator) Next(val interface{}) bool {
	_, value, ok := ri.iter.Next()
	if !ok {
		return ok
	}
	if ri.tableRoot != nil {
		pKey := encodeKey(value)
		value, ok := ri.tableRoot.Get(ri.db, pKey)
		if !ok {
			return false
		}
		ri.db.decode(*value, val)
	} else {
		ri.db.decode(value, val)
	}

	return ok
}
