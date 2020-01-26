package ebakusdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

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

// Note: negative numbers are converted to excess-k from 2-complement
//       in order to allow proper in sequence iteration over the trie
func getEncodedIndexKey(v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.Uint8:
		return []byte{v.Interface().(uint8)}, nil
	case reflect.Int8:
		return []byte{v.Interface().(uint8) ^ 1<<7}, nil
	case reflect.Uint16:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(v.Uint()))
		return b, nil
	case reflect.Int16:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(v.Uint())^1<<15)
		return b, nil
	case reflect.Uint32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v.Uint()))
		return b, nil
	case reflect.Int32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v.Uint())^1<<31)
		return b, nil
	case reflect.Uint64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, v.Uint())
		return b, nil
	case reflect.Int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.Int())^1<<63)
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
				return nil, fmt.Errorf("Unindexable field type (%s)", e.Kind())
			}
		}
		return r, nil
	case reflect.Ptr:
		if reflect.TypeOf(v.Interface()) == reflect.TypeOf(&big.Int{}) {
			bi := v.Interface().(*big.Int)
			bytes := bi.Bytes()
			ret := make([]byte, 32)
			copy(ret[32-len(bytes):], bytes)
			if bi.Sign() < 0 {
				for i := 0; i < 32; i++ {
					ret[i] ^= 0xff
				}
			}
			ret[0] ^= 0x80
			return ret, nil
		}
	}

	return nil, fmt.Errorf("Unindexable field type (%s)", v.Kind())
}

func getTableStructInstance(tbl *Table) (interface{}, error) {
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
	case "big.Int":
		return reflect.TypeOf(big.Int{}), nil
	case "*big.Int":
		return reflect.TypeOf(&big.Int{}), nil
	}
	return nil, fmt.Errorf("unsupported arg type: %s", t)
}

func isNilValue(i interface{}) bool {
	return (*[2]uintptr)(unsafe.Pointer(&i))[1] == 0
}

type Snapshot struct {
	db   *DB
	root Ptr

	objAllocated int64

	writable *simplelru.LRU

	writer sync.Mutex
}

func (s *Snapshot) GetObjAllocated() int64 {
	return atomic.LoadInt64(&s.objAllocated)
}

func (s *Snapshot) ResetObjAllocated() {
	atomic.StoreInt64(&s.objAllocated, 0)
}

func (s *Snapshot) addObjAllocated(count int) {
	atomic.AddInt64(&s.objAllocated, int64(count))
}

func (s *Snapshot) Release() {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	s.writable = nil
	s.root.NodeRelease(mm)
}

func (s *Snapshot) GetId() uint64 {
	return uint64(s.root)
}

func (s *Snapshot) GetFreeMemory() uint64 {
	return s.db.allocator.GetFree()
}

func (s *Snapshot) GetUsedMemory() uint64 {
	return s.db.allocator.GetUsed()
}

func (s *Snapshot) Get(k []byte) (*[]byte, bool) {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	return s.get(k)
}

func (s *Snapshot) get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return s.root.getNode(s.db.allocator).Get(s.db, k)
}

func (s *Snapshot) CreateTable(table string, obj interface{}) error {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	nPtr, _, err := newNode(mm)
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

	tbl := Table{
		Node:    *nPtr,
		Indexes: make([]string, 0),
		Schema:  schema,
	}

	tbl.Indexes = append(tbl.Indexes, "Id")

	v, _ := s.db.encode(tbl)
	s.insertWithNode(getTableKey(table), v, tbl.Node)

	return nil
}

func (s *Snapshot) CreateIndex(index IndexField) error {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	tPtrMarshaled, found := s.get(getTableKey(index.Table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	tbl.Indexes = append(tbl.Indexes, index.Field)

	v, _ := s.db.encode(tbl)
	s.insertWithNode(getTableKey(index.Table), v, tbl.Node)

	nPtr, _, err := newNode(mm)
	if err != nil {
		return err
	}
	v, _ = s.db.encode(nPtr)
	s.insertWithNode(index.getIndexKey(), v, *nPtr)

	return nil
}

func (s *Snapshot) HasTable(table string) bool {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	_, exists := s.get(getTableKey(table))

	return exists
}

func (s *Snapshot) Iter() *Iterator {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	iter := s.root.getNodeIterator(mm)
	return iter
}

func (s *Snapshot) Snapshot() *Snapshot {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	s.writable = nil

	s.root.getNode(mm).Retain()

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

	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()
	s.root.getNode(mm).Retain()
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

	if _, ok := s.writable.Get(*nodePtr); ok {
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

	s.writable.Add(*ncPtr, nil)

	return ncPtr
}

func (s *Snapshot) insert(nodePtr *Ptr, k, search []byte, vPtr ByteArray, vNode Ptr) (*Ptr, *ByteArray, bool) {
	if err := vPtr.checkBytesLength(); err != nil {
		return nil, nil, false
	}

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

		nc.keyPtr.Release(mm)
		nc.keyPtr = *newBytesFromSlice(mm, k)
		nc.valPtr.Release(mm)
		nc.valPtr = vPtr
		nc.valPtr.Retain(mm)
		if nc.nodePtr != vNode {
			nc.nodePtr.NodeRelease(mm)
			nc.nodePtr = vNode
		}

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
		nn.nodePtr = vNode
		nn.prefixPtr = *newBytesFromSlice(mm, search)

		nc := s.writeNode(nodePtr)
		nc.getNode(mm).edges[edgeLabel] = *nnPtr

		return nc, nil, false
	}

	child := childPtr.getNode(mm)

	// Determine longest prefix of the search key on match
	childPrefix := child.prefixPtr.getBytes(mm)
	commonPrefix := longestPrefix(search, childPrefix)
	if commonPrefix == len(childPrefix) {
		search = search[commonPrefix:]
		newChildPtr, oldVal, didUpdate := s.insert(&childPtr, k, search, vPtr, vNode)
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

	newPrefix := *newBytesFromSlice(mm, pref[commonPrefix:])
	modChild.prefixPtr.Release(mm)
	modChild.prefixPtr = newPrefix

	// If the new key is a subset, add to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.keyPtr = *newBytesFromSlice(mm, k)
		splitNode.valPtr = vPtr
		vPtr.Retain(mm)
		splitNode.nodePtr = vNode
		return ncPtr, nil, false
	}

	enPtr, en, err := newNode(mm)
	if err != nil {
		panic(err)
	}
	en.keyPtr = *newBytesFromSlice(mm, k)
	en.valPtr = vPtr
	vPtr.Retain(mm)
	en.nodePtr = vNode
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
	//n.nodePtr.NodeRelease(mm)
	n.keyPtr = child.keyPtr
	n.keyPtr.Retain(mm)
	n.valPtr = child.valPtr
	n.valPtr.Retain(mm)
	n.nodePtr = child.nodePtr
	n.nodePtr.NodeRetain(mm)

	n.edges = child.edges

	for _, edgeNode := range n.edges {
		edgeNode.NodeRetain(mm)
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
		nc.nodePtr.NodeRelease(mm)

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
	return s.InsertWithNode(k, v, 0)
}

func (s *Snapshot) InsertWithNode(k, v []byte, vp Ptr) (*[]byte, bool) {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	return s.insertWithNode(k, v, vp)
}

func (s *Snapshot) insertWithNode(k, v []byte, vp Ptr) (*[]byte, bool) {
	if err := checkBytesLength(v); err != nil {
		return nil, false
	}

	k = encodeKey(k)
	mm := s.db.allocator

	vPtr := *newBytesFromSlice(mm, v)

	s.writer.Lock()

	newRoot, oldVal, didUpdate := s.insert(&s.root, k, k, vPtr, vp)
	if newRoot != nil {
		s.root.NodeRelease(mm)
		s.root = *newRoot
	}

	mm.Unlock()
	s.db.Grow()
	mm.Lock()

	s.writer.Unlock()

	vPtr.Release(mm)

	if oldVal == nil {
		return nil, didUpdate
	}

	mm.Lock()
	val := oldVal.getBytes(mm)
	oVal := make([]byte, len(val))
	copy(oVal, val)
	oldVal.Release(mm)
	mm.Unlock()

	return &oVal, didUpdate
}

func (s *Snapshot) Delete(k []byte) bool {
	s.writer.Lock()
	defer s.writer.Unlock()

	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

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

func compKeys(a []byte, b []byte) bool {
	l := len(a)
	if l > len(b) {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if a[i] > b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (s *Snapshot) InsertObj(table string, obj interface{}) error {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	tPtrMarshaled, found := s.get(getTableKey(table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}

	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	if reflect.Ptr != reflect.TypeOf(obj).Kind() {
		return fmt.Errorf("Object has to be a pointer")
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

	if err := checkBytesLength(objMarshaled); err != nil {
		return err
	}

	k, err := getEncodedIndexKey(pv)
	if err != nil {
		return err
	}
	ek := encodeKey(k)

	s.addObjAllocated(len(objMarshaled))
	s.addObjAllocated(len(k))

	objPtr := *newBytesFromSlice(mm, objMarshaled)
	newRoot, oldVal, _ := s.insert(&tbl.Node, ek, ek, objPtr, 0)
	if *newRoot == tbl.Node {
		newRoot.NodeRelease(mm)
	}
	objPtr.Release(mm)

	if newRoot != nil {
		tbl.Node = *newRoot
		tblMarshaled, _ := s.db.encode(tbl)
		s.insertWithNode(getTableKey(table), tblMarshaled, tbl.Node)
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
		tPtrMarshaled, found := s.get(ifield.getIndexKey())
		if found == false {
			return fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		s.db.decode(*tPtrMarshaled, &tPtr)

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
			s.addObjAllocated(-len(oldIk))
			oldIk = encodeKey(oldIk)

			oldUKeys := make([][]byte, 0)
			oldUKeysMarshalled, found := tPtr.getNode(mm).Get(s.db, oldIk)
			if found {
				s.db.decode(*oldUKeysMarshalled, &oldUKeys)
			}

			var newRoot *Ptr
			var oldIVal *ByteArray

			// When multiple entries, remove the single entry and update
			if len(oldUKeys) > 1 {
				uKeys := make([][]byte, 0)
				found := false
				for _, v := range oldUKeys {
					if bytes.Equal(k, v) {
						found = true
					} else {
						uKeys = append(uKeys, v)
					}
				}
				if !found {
					return fmt.Errorf("Indexed key not found in old position")
				}

				// Order by primary key internaly
				sort.Slice(uKeys, func(i, j int) bool {
					return compKeys(uKeys[i], uKeys[j])
				})

				ivMarshaled, err := s.db.encode(uKeys)
				if err != nil {
					return err
				}

				pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
				newRoot, oldIVal, _ = s.insert(&tPtr, oldIk, oldIk, pKeyPtr, 0)
				pKeyPtr.Release(mm)

				// When single entry, remove the node
			} else {
				newRoot, oldIVal = s.delete(nil, &tPtr, oldIk)
			}

			if oldIVal != nil {
				oldIVal.Release(mm)
			}
			if *newRoot == tPtr {
				newRoot.NodeRelease(mm)
			}
			if newRoot != nil {
				tPtr = *newRoot
				tPtrMarshaled, _ := s.db.encode(tPtr)
				s.insertWithNode(ifield.getIndexKey(), tPtrMarshaled, tPtr)
			}
		}

		ik, err := getEncodedIndexKey(fv)
		if err != nil {
			return err
		}

		s.addObjAllocated(len(ik))

		ik = encodeKey(ik)

		oldKeys := make([][]byte, 0)
		oldKeysMarshalled, found := tPtr.getNode(mm).Get(s.db, ik)
		if found {
			s.db.decode(*oldKeysMarshalled, &oldKeys)
		}

		keys := append(oldKeys, k)

		// Order by primary key internaly
		sort.Slice(keys, func(i, j int) bool {
			return compKeys(keys[i], keys[j])
		})

		ivMarshaled, err := s.db.encode(keys)
		if err != nil {
			return err
		}

		pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
		newRoot, oldValue, _ := s.insert(&tPtr, ik, ik, pKeyPtr, 0)
		if oldValue != nil {
			oldValue.Release(mm)
		}
		pKeyPtr.Release(mm)
		if *newRoot == tPtr {
			newRoot.NodeRelease(mm)
		}
		if newRoot != nil {
			tPtr = *newRoot
			tPtrMarshaled, _ := s.db.encode(tPtr)
			s.insertWithNode(ifield.getIndexKey(), tPtrMarshaled, tPtr)
		}
	}

	return nil
}

func (s *Snapshot) DeleteObj(table string, id interface{}) error {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	tPtrMarshaled, found := s.get(getTableKey(table))
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

	s.addObjAllocated(-len(k))

	newRoot, oldVal := s.delete(nil, &tbl.Node, ek)
	if *newRoot == tbl.Node {
		newRoot.NodeRelease(mm)
	}
	if oldVal != nil {
		defer oldVal.Release(mm)
		s.addObjAllocated(-int(oldVal.Size))
	}

	var oldV reflect.Value
	if len(tbl.Indexes) > 1 {
		if oldVal == nil {
			return fmt.Errorf("Old value not found")
		}

		obj, err := getTableStructInstance(&tbl)
		if err != nil {
			return err
		}

		oldV = reflect.ValueOf(obj)

		oldBytes := oldVal.getBytes(mm)
		s.db.decode(oldBytes, obj)
		oldV = reflect.Indirect(oldV)
	}

	if newRoot != nil {
		tbl.Node = *newRoot
		tblMarshaled, _ := s.db.encode(tbl)
		s.insertWithNode(getTableKey(table), tblMarshaled, tbl.Node)
	}

	// Do the additional indexes
	for _, indexField := range tbl.Indexes {
		if indexField == "Id" {
			continue
		}

		ifield := IndexField{Table: table, Field: indexField}
		tPtrMarshaled, found := s.get(ifield.getIndexKey())
		if found == false {
			return fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		s.db.decode(*tPtrMarshaled, &tPtr)
		n := tPtr.getNode(mm)

		fv := oldV.FieldByName(indexField)
		if !fv.IsValid() {
			return fmt.Errorf("Object doesn't have an %s field", indexField)
		}

		ik, err := getEncodedIndexKey(fv)
		if err != nil {
			return err
		}
		s.addObjAllocated(-len(ik))
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
					okMar, _ := s.db.encode(oldKeys[i])
					s.addObjAllocated(-len(okMar))
				}
			}
			if !found {
				return fmt.Errorf("Key to be deleted not found")
			}

			// Order by primary key internaly
			sort.Slice(oldKeys, func(i, j int) bool {
				return compKeys(oldKeys[i], oldKeys[j])
			})

			ivMarshaled, err := s.db.encode(oldKeys)
			if err != nil {
				return err
			}

			pKeyPtr := *newBytesFromSlice(mm, ivMarshaled)
			newRoot, oldIVal, _ = s.insert(&tPtr, ik, ik, pKeyPtr, 0)
			pKeyPtr.Release(mm)

			// When single entry, remove the node
		} else {
			newRoot, oldIVal = s.delete(nil, &tPtr, ik)
		}

		if oldIVal != nil {
			oldIVal.Release(mm)
		}
		if *newRoot == tPtr {
			newRoot.NodeRelease(mm)
		}
		if newRoot != nil {
			tPtr = *newRoot
			tPtrMarshaled, _ := s.db.encode(tPtr)
			s.insertWithNode(ifield.getIndexKey(), tPtrMarshaled, tPtr)
		}
	}

	return nil
}

type WhereCondition int

const (
	Equal WhereCondition = iota
	NotEqual
	Smaller
	SmallerOrEqual
	Larger
	LargerOrEqual
	Like
)

type WhereField struct {
	Field     string
	Condition WhereCondition
	Value     []byte
}

func (s *Snapshot) WhereParser(input []byte) (*WhereField, error) {
	tokenizer := NewTokenizer([]string{"<", ">", "=", "==", "<=", ">=", "!=", "LIKE"})
	parts := tokenizer.Tokenize(input)

	if len(parts) == 0 {
		return nil, nil
	}

	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed where query")
	}

	var condition WhereCondition

	switch string(parts[1]) {
	case "=", "==":
		condition = Equal
	case "!=":
		condition = NotEqual
	case "<":
		condition = Smaller
	case "<=":
		condition = SmallerOrEqual
	case ">":
		condition = Larger
	case ">=":
		condition = LargerOrEqual
	case "LIKE":
		condition = Like
	}

	var val []byte
	if len(parts) >= 3 {
		val = parts[2]
	}

	return &WhereField{
		Field:     string(parts[0]),
		Condition: condition,
		Value:     val,
	}, nil
}

type OrderCondition int

const (
	ASC OrderCondition = iota
	DESC
)

type OrderField struct {
	Field string
	Order OrderCondition
}

func (s *Snapshot) OrderParser(input []byte) (*OrderField, error) {
	tokenizer := NewTokenizer([]string{"ASC", "DESC"})
	parts := tokenizer.Tokenize(input)

	if len(parts) == 0 {
		return nil, nil
	}

	if len(parts) < 1 {
		return nil, fmt.Errorf("malformed order query")
	}

	order := ASC
	if len(parts) == 2 && string(parts[1]) == "DESC" {
		order = DESC
	}

	return &OrderField{
		Field: string(parts[0]),
		Order: order,
	}, nil
}

func (s *Snapshot) Select(table string, args ...interface{}) (*ResultIterator, error) {
	mm := s.db.allocator
	mm.Lock()
	defer mm.Unlock()

	tPtrMarshaled, found := s.get(getTableKey(table))
	if found == false {
		return nil, fmt.Errorf("Unknown table")
	}
	var tbl Table
	s.db.decode(*tPtrMarshaled, &tbl)

	var iter *Iterator
	var tblNode Ptr

	var whereClause *WhereField
	var orderClause *OrderField

	if len(args) >= 1 && !isNilValue(args[0]) {
		whereClause = args[0].(*WhereField)
	}

	if len(args) >= 2 && !isNilValue(args[1]) {
		tempOrderClause := args[1].(*OrderField)
		for _, indexField := range tbl.Indexes {
			if tempOrderClause.Field == indexField {
				orderClause = tempOrderClause
				break
			}
		}
	}

	if orderClause == nil {
		orderClause = &OrderField{
			Field: "Id",
			Order: ASC,
		}
	}

	if orderClause.Field == "Id" {
		iter = tbl.Node.getNodeIterator(mm)
	} else {
		ifield := IndexField{Table: table, Field: orderClause.Field}
		tPtrMarshaled, found := s.get(ifield.getIndexKey())
		if found == false {
			return nil, fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		s.db.decode(*tPtrMarshaled, &tPtr)
		iter = tPtr.getNodeIterator(mm)

		tblNode = tbl.Node
	}

	return &ResultIterator{
		db:          s.db,
		iter:        iter,
		tableRoot:   tblNode,
		whereClause: whereClause,
		orderClause: orderClause,
	}, nil
}

func (s *Snapshot) Root() *Ptr {
	return &s.root
}

func (s *Snapshot) RootNode() *Node {
	return s.Root().getNode(s.db.allocator)
}

func (s *Snapshot) PrintTree() {
	fmt.Println("<>")
	s.RootNode().printTree(s.db.allocator, 0, "", false)
}

func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
