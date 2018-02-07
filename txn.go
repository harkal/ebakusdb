package ebakusdb

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

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
	v, _ := t.db.encode(nPtr)
	t.Insert(getTableKey(table), v)
	return nil
}

func (t *Txn) CreateIndex(index IndexField) error {
	nPtr, _, err := newNode(t.db.allocator)
	if err != nil {
		return err
	}
	v, _ := t.db.encode(nPtr)
	t.Insert(index.getIndexKey(), v)

	return nil
}

func (t *Txn) InsertObj(table string, obj interface{}, args ...interface{}) error {
	tPtrMarshaled, found := t.Get(getTableKey(table))
	if found == false {
		return fmt.Errorf("Unknown table")
	}
	var tPtr Ptr
	t.db.decode(*tPtrMarshaled, &tPtr)

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
	k = encodeKey(k)

	objPtr := *newBytesFromSlice(mm, objMarshaled)
	newRoot, _, _ := t.insert(&tPtr, k, k, objPtr)
	objPtr.Release(mm)
	if newRoot != nil {
		tPtr.NodeRelease(mm)
		tPtr = *newRoot
		tPtrMarshaled, _ := t.db.encode(tPtr)
		t.Insert(getTableKey(table), tPtrMarshaled)
	}

	// Do the additional indexes

	return nil
}
