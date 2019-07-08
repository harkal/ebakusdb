package ebakusdb

import (
	"bytes"
	"reflect"
	"strings"

	"github.com/harkal/ebakusdb/balloc"
)

type edge struct {
	key  byte
	node *Node
}

type edges []edge

type Iterator struct {
	rootNode *Node
	node     *Node
	stack    []edges
	mm       balloc.MemoryManager
}

func (i *Iterator) SeekPrefix(prefix []byte) {
	prefix = encodeKey(prefix)
	i.stack = nil
	n := i.node
	if n == nil {
		n = i.rootNode
	}
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

func (i *Iterator) Prev() ([]byte, []byte, bool) {
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
		for k, _ := range elem.edges {
			nPtr := elem.edges[len(elem.edges)-k-1]
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
	db      *DB
	iter    *Iterator
	entries [][]byte

	tableRoot *Node

	whereClause *WhereField
	orderClause *OrderField
}

func (ri *ResultIterator) Next(val interface{}) bool {
	nextIter := func() ([]byte, []byte, bool) {
		if ri.orderClause.Order == DESC {
			return ri.iter.Prev()
		}
		return ri.iter.Next()
	}

	var whereObjValue reflect.Value
	var whereValueType reflect.Type

	// when Where=LIKE and we want to compare non string values,
	// run a SeekPrefix ONCE and remove the whereClause for future use
	if ri.whereClause != nil && ri.whereClause.Condition == Like && ri.whereClause.Field == ri.orderClause.Field {
		obj := reflect.ValueOf(val)
		obj = reflect.Indirect(obj)
		whereObjValue = obj.FieldByName(ri.whereClause.Field)
		if !whereObjValue.IsValid() {
			return ri.Next(val)
		}

		whereValueType = reflect.TypeOf(whereObjValue.Interface())

		if whereValueType.Kind() != reflect.String {
			ri.iter.SeekPrefix(ri.whereClause.Value)
			ri.whereClause = nil
			return ri.Next(val)
		}
	}

	if ri.tableRoot != nil {
		if len(ri.entries) == 0 {
			_, value, ok := nextIter()
			if !ok {
				return false
			}
			ri.db.decode(value, &ri.entries)
			return ri.Next(val)
		}

		var ik []byte
		if ri.orderClause.Order == DESC {
			ik = ri.entries[len(ri.entries)-1]
			ri.entries = ri.entries[:len(ri.entries)-1]

		} else {
			ik = ri.entries[0]
			ri.entries = ri.entries[1:]
		}

		ik = encodeKey(ik)
		value, ok := ri.tableRoot.Get(ri.db, ik)
		if !ok {
			return false
		}
		ri.db.decode(*value, val)
	} else {
		_, value, ok := nextIter()
		if !ok {
			return false
		}
		ri.db.decode(value, val)
	}

	if ri.whereClause != nil {
		if !whereObjValue.IsValid() || whereValueType == nil {
			obj := reflect.ValueOf(val)
			obj = reflect.Indirect(obj)

			whereObjValue = obj.FieldByName(ri.whereClause.Field)
			if !whereObjValue.IsValid() {
				return ri.Next(val)
			}

			whereValueType = reflect.TypeOf(whereObjValue.Interface())
		}

		whereValue := toGoType(whereValueType.Kind(), ri.whereClause.Value)
		whereValueR := reflect.ValueOf(whereValue)

		// handle edge case, where an empty Id field is set
		if whereValueType.Kind() == reflect.Slice && whereObjValue.Len() == 0 && whereValue == nil {
			return true
		}

		var fn ComparisonFunction

		switch ri.whereClause.Condition {
		case Equal:
			fn = eq
		case NotEqual:
			fn = ne
		case Smaller:
			fn = lt
		case SmallerOrEqual:
			fn = le
		case Larger:
			fn = gt
		case LargerOrEqual:
			fn = ge
		case Like:
			if whereValueType.Kind() == reflect.String {
				if !strings.Contains(whereObjValue.Interface().(string), whereValueR.Interface().(string)) {
					return ri.Next(val)
				}
			}
		}

		// NOTE: fn == nil, return false

		if fn != nil {
			if ok, _ := fn(whereObjValue, whereValueR); !ok {
				return ri.Next(val)
			}
		}
	}

	return true
}
