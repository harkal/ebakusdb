package ebakusdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
)

type Table struct {
	Indexes []string
	Node    Ptr
	Schema  string
}

type IndexField struct {
	Table string
	Field string
}

func (i *IndexField) getIndexKey() []byte {
	return []byte(i.Table + "." + i.Field)
}

func getTableKey(table string) []byte {
	return []byte("t_" + table)
}

func getEncodedIndexKey(v reflect.Value) ([]byte, error) {

	switch v.Kind() {
	case reflect.Uint64, reflect.Int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, v.Uint())
		return b, nil
	case reflect.String:
		return []byte(v.String()), nil
	case reflect.Slice:
		return v.Bytes(), nil
	case reflect.Array:
		if v.Len() == 0 {
			return nil, fmt.Errorf("Empty array unindexable")
		}
		r := make([]byte, 0)
		for i := 0; i < v.Len(); i++ {
			e := v.Index(i)
			switch e.Kind() {
			case reflect.Uint8:
				r = append(r, byte(e.Uint()))
			default:
				return nil, fmt.Errorf("Unindexable field type")
			}
		}
		return r, nil
	default:
		return nil, fmt.Errorf("Unindexable field type")
	}
}

func getTableInstance(tbl *Table) (interface{}, error) {
	fields := strings.Split(tbl.Schema, ",")

	if len(fields) < 1 {
		return nil, fmt.Errorf("No fields in struct schema")
	}

	var sfields []reflect.StructField
	for i, f := range fields {
		a := strings.Split(f, " ")
		aName, aType := a[0], a[1]

		t, err := getReflectTypeFromString(aType)
		if err != nil {
			return nil, err
		}

		sf := reflect.StructField{
			Name:  aName,
			Type:  t,
			Index: []int{i},
		}
		sfields = append(sfields, sf)
	}

	st := reflect.StructOf(sfields)
	so := reflect.New(st)
	return so.Interface(), nil
}

func getReflectTypeFromString(t string) (reflect.Type, error) {
	switch t {
	case "bool":
		return reflect.TypeOf(true), nil
	case "int":
		return reflect.TypeOf(int(0)), nil
	case "int8":
		return reflect.TypeOf(int8(0)), nil
	case "int16":
		return reflect.TypeOf(int16(0)), nil
	case "int32":
		return reflect.TypeOf(int32(0)), nil
	case "int64":
		return reflect.TypeOf(int64(0)), nil
	case "uint":
		return reflect.TypeOf(uint(0)), nil
	case "uint8":
		return reflect.TypeOf(uint8(0)), nil
	case "uint16":
		return reflect.TypeOf(uint16(0)), nil
	case "uint32":
		return reflect.TypeOf(uint32(0)), nil
	case "uint64":
		return reflect.TypeOf(uint64(0)), nil
	case "uintptr":
		return reflect.TypeOf(uintptr(0)), nil
	case "float32":
		return reflect.TypeOf(float32(0)), nil
	case "float64":
		return reflect.TypeOf(float64(0)), nil
	case "complex64":
		return reflect.TypeOf(complex64(0)), nil
	case "complex128":
		return reflect.TypeOf(complex128(0)), nil
	case "string":
		return reflect.TypeOf(""), nil
	}
	return nil, fmt.Errorf("unsupported arg type: %s", t)
}

type Snapshot struct {
	db   *DB
	root Ptr

	writable *simplelru.LRU

	writer sync.Mutex
}

func (s *Snapshot) Release() {
	s.writable = nil
	s.root.NodeRelease(s.db.allocator)
}

func (s *Snapshot) GetId() uint64 {
	return uint64(s.root)
}

func (s *Snapshot) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return s.root.getNode(s.db.allocator).Get(s.db, k)
}

func (s *Snapshot) CreateTable(table string, obj interface{}) error {
	nPtr, _, err := newNode(s.db.allocator)
	if err != nil {
		return err
	}

	schema := ""
	if reflect.Ptr == reflect.TypeOf(obj).Kind() {
		st := reflect.ValueOf(obj).Elem()

		if reflect.Struct == st.Kind() {
			num := st.NumField()

			inputs := make([]string, num)
			for i := 0; i < num; i++ {
				f := st.Field(i)
				inputs[i] = fmt.Sprintf("%v %v", st.Type().Field(i).Name, f.Type())
			}

			schema = strings.Join(inputs, ",")
		}
	}

	tlb := Table{
		Node:    *nPtr,
		Indexes: make([]string, 0),
		Schema:  schema,
	}

	v, _ := s.db.encode(tlb)
	s.Insert(getTableKey(table), v)
	return nil
}

func (s *Snapshot) CreateIndex(index IndexField) error {
	tPtrMarshaled, found := s.Get(getTableKey(index.Table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	tbl.Indexes = append(tbl.Indexes, index.Field)

	v, _ := s.db.encode(tbl)
	s.Insert(getTableKey(index.Table), v)

	nPtr, _, err := newNode(s.db.allocator)
	if err != nil {
		return err
	}
	v, _ = s.db.encode(nPtr)
	s.Insert(index.getIndexKey(), v)

	return nil
}

func (s *Snapshot) HasTable(table string) bool {
	_, exists := s.Get(getTableKey(table))

	return exists
}

func (s *Snapshot) Iter() *Iterator {
	iter := s.root.getNode(s.db.allocator).Iterator(s.db.allocator)
	return iter
}

func (s *Snapshot) Snapshot() *Snapshot {
	s.root.getNode(s.db.allocator).Retain()

	return &Snapshot{
		db:   s.db,
		root: s.root,
	}
}

func (s *Snapshot) ResetTo(to *Snapshot) {
	if s.GetId() == to.GetId() {
		return
	}
	s.Release()
	s.root = to.root
	s.root.getNode(s.db.allocator).Retain()
}

func (s *Snapshot) writeNode(nodePtr *Ptr) *Ptr {
	mm := s.db.allocator
	if s.writable == nil {
		lru, err := simplelru.NewLRU(defaultWritableCache, nil)
		if err != nil {
			panic(err)
		}
		s.writable = lru
	}

	n := nodePtr.getNode(mm)

	// if _, ok := s.writable.Get(*nodePtr); ok {
	// 	//println("hit", t.writable.Len())
	// 	n.Retain()
	// 	return nodePtr
	// }

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

	s.writable.Add(*ncPtr, nil)

	return ncPtr
}

func (s *Snapshot) insert(nodePtr *Ptr, k, search []byte, vPtr ByteArray) (*Ptr, *ByteArray, bool) {
	mm := s.db.allocator
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

		ncPtr := s.writeNode(nodePtr)
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

		nc := s.writeNode(nodePtr)
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
		newChildPtr, oldVal, didUpdate := s.insert(&childPtr, k, search, vPtr)
		if newChildPtr != nil {
			ncPtr := s.writeNode(nodePtr)
			nc := ncPtr.getNode(mm)
			nc.edges[edgeLabel].NodeRelease(mm)
			nc.edges[edgeLabel] = *newChildPtr
			return ncPtr, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// Split the node
	ncPtr := s.writeNode(nodePtr)
	nc := ncPtr.getNode(mm)

	splitNodePtr, splitNode, err := newNode(mm)
	if err != nil {
		panic(err)
	}

	splitNode.prefixPtr = *newBytesFromSlice(mm, search[:commonPrefix])

	// Restore the existing child node
	modChildPtr := s.writeNode(&childPtr)
	modChild := modChildPtr.getNode(mm)
	pref := modChild.prefixPtr.getBytes(mm)

	splitNode.edges[pref[commonPrefix]] = *modChildPtr

	nc.edges[edgeLabel].NodeRelease(mm)
	nc.edges[edgeLabel] = *splitNodePtr

	modChild.prefixPtr = *newBytesFromSlice(mm, pref[commonPrefix:])

	// If the new key is a subset, add to this node
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

// mergeChild merges a trie node with its parent node.
//
// NOTE: don't merge back to the root trie node,
//       as insert() doesn't handle search lookup properly.
func (s *Snapshot) mergeChild(n *Node) {
	mm := s.db.allocator

	childPtr := n.getFirstChild()
	child := childPtr.getNode(mm)

	if !n.hasOneChild() || n.isLeaf() {
		panic("Can't merge non leaf child node")
	}

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

func (s *Snapshot) delete(parentPtr, nPtr *Ptr, search []byte) (*Ptr, *ByteArray) {
	mm := s.db.allocator
	n := nPtr.getNode(mm)

	// Check for key exhaustion
	if len(search) == 0 {
		if !n.isLeaf() {
			return nil, nil
		}

		var oldVal ByteArray
		oldVal = n.valPtr
		oldVal.Retain(mm)

		// Remove the leaf node
		ncPtr := s.writeNode(nPtr)
		nc := ncPtr.getNode(mm)
		nc.keyPtr.Release(mm)
		nc.valPtr.Release(mm)

		// Check if this node should be merged
		if *nPtr != s.root && nc.hasOneChild() && parentPtr != nil {
			s.mergeChild(nc)
		}

		return ncPtr, &oldVal
	}

	edgeLabel := search[0]
	childPtr := n.edges[edgeLabel]
	if childPtr.isNull() {
		return nil, nil
	}

	child := childPtr.getNode(mm)
	childPrefix := child.prefixPtr.getBytes(mm)

	if !bytes.HasPrefix(search, childPrefix) {
		return nil, nil
	}

	// Consume the search prefix
	search = search[len(childPrefix):]
	newChildPtr, oldVal := s.delete(nPtr, &childPtr, search)
	if newChildPtr == nil {
		return nil, oldVal
	}

	newChild := newChildPtr.getNode(mm)

	ncPtr := s.writeNode(nPtr)
	nc := ncPtr.getNode(mm)

	nc.edges[edgeLabel].NodeRelease(mm)
	if newChild.isLeaf() == false && newChild.getFirstChild() == 0 {
		nc.edges[edgeLabel] = 0
		if *nPtr != s.root && parentPtr != nil && nc.hasOneChild() && !nc.isLeaf() {
			s.mergeChild(nc)
		}
		newChildPtr.NodeRelease(mm)
	} else {
		nc.edges[edgeLabel] = *newChildPtr
	}

	return ncPtr, oldVal
}

func (s *Snapshot) Insert(k, v []byte) (*[]byte, bool) {
	k = encodeKey(k)
	mm := s.db.allocator

	vPtr := *newBytesFromSlice(mm, v)

	s.writer.Lock()

	newRoot, oldVal, didUpdate := s.insert(&s.root, k, k, vPtr)
	if newRoot != nil {
		s.root.NodeRelease(mm)
		s.root = *newRoot
	}

	s.db.Grow()

	s.writer.Unlock()

	vPtr.Release(mm)

	if oldVal == nil {
		return nil, didUpdate
	}

	val := oldVal.getBytes(mm)
	oVal := make([]byte, len(val))
	copy(oVal, val)
	oldVal.Release(mm)

	return &val, didUpdate
}

func (s *Snapshot) Delete(k []byte) bool {
	s.writer.Lock()
	defer s.writer.Unlock()

	mm := s.db.allocator
	k = encodeKey(k)
	newRoot, oldVal := s.delete(nil, &s.root, k)
	if oldVal != nil {
		oldVal.Release(mm)
	}

	if newRoot != nil {
		s.root.NodeRelease(mm)
		s.root = *newRoot
		return true
	}
	return false
}

func (s *Snapshot) InsertObj(table string, obj interface{}) error {
	tPtrMarshaled, found := s.Get(getTableKey(table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	if reflect.Ptr != reflect.TypeOf(obj).Kind() {
		panic("Object has to be a pointer")
	}

	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)

	pv := v.FieldByName("Id")
	if !pv.IsValid() {
		return fmt.Errorf("Object doesn't have an id field")
	}

	objMarshaled, err := s.db.encode(obj)
	if err != nil {
		return err
	}

	mm := s.db.allocator

	k, err := getEncodedIndexKey(pv)
	if err != nil {
		return err
	}
	ek := encodeKey(k)

	objPtr := *newBytesFromSlice(mm, objMarshaled)
	newRoot, oldVal, _ := s.insert(&tbl.Node, ek, ek, objPtr)
	objPtr.Release(mm)

	if newRoot != nil {
		tbl.Node = *newRoot
		tblMarshaled, _ := s.db.encode(tbl)
		s.Insert(getTableKey(table), tblMarshaled)
	}

	var oldV reflect.Value
	if oldVal != nil {
		oldBytes := oldVal.getBytes(mm)
		t := reflect.TypeOf(obj)
		oldV = reflect.New(t)
		s.db.decode(oldBytes, oldV.Interface())
		oldV = reflect.Indirect(oldV)

		defer oldVal.Release(mm)
	}

	// Do the additional indexes
	for _, indexField := range tbl.Indexes {
		if indexField == "Id" {
			continue
		}

		ifield := IndexField{Table: table, Field: indexField}
		tPtrMarshaled, found := s.Get(ifield.getIndexKey())
		if found == false {
			return fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		s.db.decode(*tPtrMarshaled, &tPtr)
		n := tPtr.getNode(mm)
		n.Retain()

		fv := v.FieldByName(indexField)
		if !fv.IsValid() {
			return fmt.Errorf("Object doesn't have an %s field", indexField)
		}

		// Update old indexes
		if oldVal != nil {
			oldIndexField := oldV.Elem().FieldByName(indexField)
			if !oldIndexField.IsValid() {
				return fmt.Errorf("Old object doesn't have an %s field", indexField)
			}

			if fv.Interface() == oldIndexField.Interface() {
				continue
			}

			oldIk, err := getEncodedIndexKey(oldIndexField)
			if err != nil {
				return err
			}
			oldIk = encodeKey(oldIk)

			oldUKeys := make([][]byte, 0)
			oldUKeysMarshalled, found := n.Get(s.db, oldIk)
			if found {
				s.db.decode(*oldUKeysMarshalled, &oldUKeys)
			}

			var newRoot *Ptr
			var oldIVal *ByteArray

			// When multiple entries, remove the single entry and update
			if len(oldUKeys) > 1 {
				found := false
				for i, v := range oldUKeys {
					if bytes.Equal(k, v) {
						oldUKeys = append(oldUKeys[:i], oldUKeys[i+1:]...)
						found = true
					}
				}
				if !found {
					return fmt.Errorf("Indexed key not found in old position")
				}

				ivMarshaled, err := s.db.encode(oldUKeys)
				if err != nil {
					return err
				}

				pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
				newRoot, oldIVal, _ = s.insert(&tPtr, oldIk, oldIk, pKeyPtr)
				pKeyPtr.Release(mm)

				// When single entry, remove the node
			} else {
				newRoot, oldIVal = s.delete(nil, &tPtr, oldIk)
			}

			if oldIVal != nil {
				oldIVal.Release(mm)
			}
			if newRoot != nil {
				tPtr.NodeRelease(mm)
				tPtr = *newRoot
			}
		}

		ik, err := getEncodedIndexKey(fv)
		if err != nil {
			return err
		}
		ik = encodeKey(ik)

		oldKeys := make([][]byte, 0)
		oldKeysMarshalled, found := n.Get(s.db, ik)
		if found {
			s.db.decode(*oldKeysMarshalled, &oldKeys)
		}
		keys := append(oldKeys, k)
		ivMarshaled, err := s.db.encode(keys)
		if err != nil {
			return err
		}

		pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
		newRoot, _, _ := s.insert(&tPtr, ik, ik, pKeyPtr)
		pKeyPtr.Release(mm)
		if newRoot != nil {
			tPtr.NodeRelease(mm)
			tPtr = *newRoot
			tPtrMarshaled, _ := s.db.encode(tPtr)
			s.Insert(ifield.getIndexKey(), tPtrMarshaled)
		}
	}

	return nil
}

func (s *Snapshot) DeleteObj(table string, id interface{}) error {
	tPtrMarshaled, found := s.Get(getTableKey(table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	k, err := getEncodedIndexKey(reflect.ValueOf(id))
	if err != nil {
		return err
	}
	ek := encodeKey(k)

	mm := s.db.allocator

	newRoot, oldVal := s.delete(nil, &tbl.Node, ek)
	if oldVal != nil {
		defer oldVal.Release(mm)
	}
	var oldV reflect.Value
	if len(tbl.Indexes) > 0 {
		if oldVal == nil {
			return fmt.Errorf("Old value not found")
		}

		obj, err := getTableInstance(&tbl)
		if err != nil {
			return err
		}

		oldV = reflect.ValueOf(obj)

		oldBytes := oldVal.getBytes(mm)
		s.db.decode(oldBytes, obj)
		oldV = reflect.Indirect(oldV)
	}

	if newRoot != nil {
		tbl.Node.NodeRelease(mm)
		tbl.Node = *newRoot
		tblMarshaled, _ := s.db.encode(tbl)
		s.Insert(getTableKey(table), tblMarshaled)
	}

	// Do the additional indexes
	for _, indexField := range tbl.Indexes {
		if indexField == "Id" {
			continue
		}

		ifield := IndexField{Table: table, Field: indexField}
		tPtrMarshaled, found := s.Get(ifield.getIndexKey())
		if found == false {
			return fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		s.db.decode(*tPtrMarshaled, &tPtr)
		n := tPtr.getNode(mm)
		n.Retain()

		fv := oldV.FieldByName(indexField)
		if !fv.IsValid() {
			return fmt.Errorf("Object doesn't have an %s field", indexField)
		}

		ik, err := getEncodedIndexKey(fv)

		if err != nil {
			return err
		}
		ik = encodeKey(ik)

		oldKeys := make([][]byte, 0)
		oldKeysMarshalled, found := n.Get(s.db, ik)
		if found {
			s.db.decode(*oldKeysMarshalled, &oldKeys)
		}

		var newRoot *Ptr
		var oldIVal *ByteArray

		// When multiple entries, remove the single entry and update
		if len(oldKeys) > 1 {
			found := false
			for i, v := range oldKeys {
				if bytes.Equal(k, v) {
					oldKeys = append(oldKeys[:i], oldKeys[i+1:]...)
					found = true
				}
			}
			if !found {
				return fmt.Errorf("Key to be deleted not found")
			}

			ivMarshaled, err := s.db.encode(oldKeys)
			if err != nil {
				return err
			}

			pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
			newRoot, oldIVal, _ = s.insert(&tPtr, ik, ik, pKeyPtr)
			pKeyPtr.Release(mm)

			// When single entry, remove the node
		} else {
			newRoot, oldIVal = s.delete(nil, &tPtr, ik)
		}

		if oldIVal != nil {
			oldIVal.Release(mm)
		}
		if newRoot != nil {
			tPtr.NodeRelease(mm)
			tPtr = *newRoot
			tPtrMarshaled, _ := s.db.encode(tPtr)
			s.Insert(ifield.getIndexKey(), tPtrMarshaled)
		}
	}

	return nil
}

func (s *Snapshot) Select(table string, args ...interface{}) (*ResultIterator, error) {
	tPtrMarshaled, found := s.Get(getTableKey(table))
	if found == false {
		return nil, fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	var iter *Iterator
	var tblNode *Node

	if len(args) == 0 {
		iter = tbl.Node.getNode(s.db.allocator).Iterator(s.db.allocator)
	} else if len(args) > 0 {
		indexField, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("Field names should be strings")
		}

		var v reflect.Value
		if len(args) >= 2 {
			indexValue := args[1]
			v = reflect.ValueOf(indexValue)
			v = reflect.Indirect(v)
		}

		if indexField == "Id" {
			iter = tbl.Node.getNode(s.db.allocator).Iterator(s.db.allocator)
			if len(args) >= 2 {
				prefix, err := getEncodedIndexKey(v)
				if err != nil {
					return nil, err
				}
				iter.SeekPrefix(prefix)
			}
		} else {
			ifield := IndexField{Table: table, Field: indexField}
			tPtrMarshaled, found := s.Get(ifield.getIndexKey())
			if found == false {
				return nil, fmt.Errorf("Unknown index")
			}
			var tPtr Ptr
			s.db.decode(*tPtrMarshaled, &tPtr)
			iter = tPtr.getNode(s.db.allocator).Iterator(s.db.allocator)

			if len(args) >= 2 {
				prefix, err := getEncodedIndexKey(v)
				if err != nil {
					return nil, err
				}
				iter.SeekPrefix(prefix)
			}

			tblNode = tbl.Node.getNode(s.db.allocator)
		}
	} else {
		return nil, fmt.Errorf("Bad query")
	}

	return &ResultIterator{
		db:        s.db,
		iter:      iter,
		tableRoot: tblNode,
	}, nil
}

func (s *Snapshot) Root() *Ptr {
	return &s.root
}

func (s *Snapshot) RootNode() *Node {
	return s.Root().getNode(s.db.allocator)
}

func (s *Snapshot) printTree() {
	s.RootNode().printTree(s.db.allocator, 0)
}

func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
