package ebakusdb

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

type Table struct {
	Indexes []string
	Node    Ptr
}

type IndexField struct {
	table string
	field string
}

func (i *IndexField) getIndexKey() []byte {
	return []byte(i.table + "." + i.field)
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
	default:
		return nil, fmt.Errorf("Unindexable field type")
	}
}

func (t *Txn) CreateTable(table string) error {
	nPtr, _, err := newNode(t.db.allocator)
	if err != nil {
		return err
	}

	tlb := Table{
		Node:    *nPtr,
		Indexes: make([]string, 0),
	}

	v, _ := t.db.encode(tlb)
	t.Insert(getTableKey(table), v)
	return nil
}

func (t *Txn) CreateIndex(index IndexField) error {
	tPtrMarshaled, found := t.Get(getTableKey(index.table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	t.db.decode(*tPtrMarshaled, &tbl)

	tbl.Indexes = append(tbl.Indexes, index.field)

	v, _ := t.db.encode(tbl)
	t.Insert(getTableKey(index.table), v)

	nPtr, _, err := newNode(t.db.allocator)
	if err != nil {
		return err
	}
	v, _ = t.db.encode(nPtr)
	t.Insert(index.getIndexKey(), v)

	return nil
}

func (t *Txn) InsertObj(table string, obj interface{}) error {
	tPtrMarshaled, found := t.Get(getTableKey(table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tbl Table
	t.db.decode(*tPtrMarshaled, &tbl)

	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)

	fv := v.FieldByName("Id")
	if !fv.IsValid() {
		return fmt.Errorf("Object doesn't have an id field")
	}

	objMarshaled, err := t.db.encode(obj)
	if err != nil {
		return err
	}

	mm := t.db.allocator

	k, err := getEncodedIndexKey(fv)
	if err != nil {
		return err
	}
	ek := encodeKey(k)

	objPtr := *newBytesFromSlice(mm, objMarshaled)
	newRoot, _, _ := t.insert(&tbl.Node, ek, ek, objPtr)
	objPtr.Release(mm)
	if newRoot != nil {
		tbl.Node.NodeRelease(mm)
		tbl.Node = *newRoot
		tblMarshaled, _ := t.db.encode(tbl)
		t.Insert(getTableKey(table), tblMarshaled)
	}

	// Do the additional indexes
	for _, indexField := range tbl.Indexes {
		if indexField == "Id" {
			continue
		}

		ifield := IndexField{table: table, field: indexField}
		tPtrMarshaled, found := t.Get(ifield.getIndexKey())
		if found == false {
			return fmt.Errorf("Unknown index")
		}
		var tPtr Ptr
		t.db.decode(*tPtrMarshaled, &tPtr)

		fv := v.FieldByName(indexField)
		if !fv.IsValid() {
			return fmt.Errorf("Object doesn't have an %s field", indexField)
		}

		ik, err := getEncodedIndexKey(fv)
		if err != nil {
			return err
		}
		ik = encodeKey(ik)

		pKeyPtr := *newBytesFromSlice(mm, k)
		newRoot, _, _ := t.insert(&tPtr, ik, ik, pKeyPtr)
		pKeyPtr.Release(mm)
		if newRoot != nil {
			tPtr.NodeRelease(mm)
			tPtr = *newRoot
			tPtrMarshaled, _ := t.db.encode(tPtr)
			t.Insert(ifield.getIndexKey(), tPtrMarshaled)
		}
	}

	return nil
}

func (t *Txn) Select(table string, args ...interface{}) (*ResultIterator, error) {
	tPtrMarshaled, found := t.Get(getTableKey(table))
	if found == false {
		return nil, fmt.Errorf("Unknown table")
	}
	var tbl Table
	t.db.decode(*tPtrMarshaled, &tbl)

	var iter *Iterator
	var tblNode *Node

	if len(args) == 0 {
		iter = tbl.Node.getNode(t.db.allocator).Iterator(t.db.allocator)
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
			iter = tbl.Node.getNode(t.db.allocator).Iterator(t.db.allocator)
			if len(args) >= 2 {
				prefix, err := getEncodedIndexKey(v)
				if err != nil {
					return nil, err
				}
				iter.SeekPrefix(prefix)
			}
		} else {
			ifield := IndexField{table: table, field: indexField}
			tPtrMarshaled, found := t.Get(ifield.getIndexKey())
			if found == false {
				return nil, fmt.Errorf("Unknown index")
			}
			var tPtr Ptr
			t.db.decode(*tPtrMarshaled, &tPtr)
			iter = tPtr.getNode(t.db.allocator).Iterator(t.db.allocator)

			if len(args) >= 2 {
				prefix, err := getEncodedIndexKey(v)
				if err != nil {
					return nil, err
				}
				iter.SeekPrefix(prefix)
			}

			tblNode = tbl.Node.getNode(t.db.allocator)
		}
	} else {
		return nil, fmt.Errorf("Bad query")
	}

	return &ResultIterator{
		db:        t.db,
		iter:      iter,
		tableRoot: tblNode,
	}, nil
}
