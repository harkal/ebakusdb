package ebakusdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

//var src = rand.NewSource(time.Now().UnixNano())
var src = rand.NewSource(1)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func Test_Open(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}
}

func Test_Tnx(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.Txn()
	old, update := t.Insert([]byte("key"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed value already there")
	}
	fmt.Println("old:", old)
	old, update = t.Insert([]byte("key"), []byte("va"))
	if update == false || string(*old) != "value" {
		test.Fatal("Update failed")
	}
	fmt.Println("old:", old)

	old, update = t.Insert([]byte("harry"), []byte("kalogirou"))
	if update == true {
		test.Fatal("Update failed")
	}

	if v, _ := t.Get([]byte("key")); string(*v) != "va" {
		test.Fatalf("Get failed (got %s)", v)
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if v, _ := db.Get([]byte("key")); string(*v) != "va" {
		test.Fatalf("Get failed (got %s)", v)
	}

	if v, _ := db.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	t = db.Txn()
	old, update = t.Insert([]byte("harry"), []byte("Kal"))
	if update == false {
		test.Fatal("Insert failed")
	}

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); string(*v) != "Kal" {
		test.Fatalf("Get failed (got %s)", v)
	}
}

func Test_SnapshotTnx(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.Txn()
	_, update := t.Insert([]byte("key"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed value already there")
	}

	_, update = t.Insert([]byte("harry"), []byte("kalogirou"))
	if update == true {
		test.Fatal("Update failed")
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	snapshot := db.Snapshot(0)

	t = db.Txn()
	_, update = t.Insert([]byte("harry"), []byte("Kal"))
	if update == false {
		test.Fatal("Insert failed")
	}

	if v, _ := db.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	tnx := snapshot.Txn()

	if v, _ := tnx.Get([]byte("key")); string(*v) != "value" {
		test.Fatalf("Get failed (got '%s')", string(*v))
	}

	// Change should not be visible on this snapshot
	if v, _ := tnx.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	// But should be visible here
	if v, _ := db.Get([]byte("harry")); string(*v) != "Kal" {
		test.Fatalf("Get failed (got %s)", v)
	}
}

func Test_Get2(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}
	mm := db.allocator
	free := db.allocator.GetFree()

	fmt.Printf("Start: %d\n", free)

	if db.header.root.getNode(mm).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	t := db.Txn()

	if db.header.root.getNode(mm).refCount != 2 {
		test.Fatal("incorrect refcount")
	}

	println("-------------------------------- before insert")

	_, update := t.Insert([]byte("key"), []byte("value the big universe dude"))
	if update == true {
		test.Fatal("Insert failed")
	}

	var deleted bool
	/*
		println("-------------------------------- before delete")

		deleted := t.Delete([]byte("key"))
		if deleted != true {
			test.Fatal("Delete failed")
		}*/

	println("-------------------------------- before commit")

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if db.header.root.getNode(mm).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	if db.header.root.getNode(mm).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	println("--------------------------------")

	t = db.Txn()
	_, update = t.Insert([]byte("harry"), []byte("NEW VALUE"))
	if update == true {
		test.Fatal("Insert failed")
	}

	println("-------------------------------- before commit")

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	println("--------------------------------")

	t = db.Txn()
	_, update = t.Insert([]byte("bobby"), []byte("NEW"))
	if update == true {
		test.Fatal("Insert failed")
	}

	println("-------------------------------- before delete")

	deleted = t.Delete([]byte("key"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	println("-------------------------------- before delete")

	deleted = t.Delete([]byte("harry"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	println("-------------------------------- before delete")

	deleted = t.Delete([]byte("bobby"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	println("-------------------------------- before commit")

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	/*	if v, _ := db.Get([]byte("key")); string(*v) != "value" {
			test.Fatal("Get failed")
		}
	*/
	//db.header.root.NodeRelease(mm)

	println("-------------------------------- FINISH")

	fmt.Printf("%d %d (%d)\n", free, db.allocator.GetFree(), int(free)-int(db.allocator.GetFree()))

}

func Test_Get_KeySubset(test *testing.T) {

	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.Txn()
	_, update := t.Insert([]byte("key_long"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed")
	}
	_, update = t.Insert([]byte("key"), []byte("value2"))
	if update == true {
		test.Fatal("Insert failed")
	}
	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if v, _ := db.Get([]byte("key_long")); string(*v) != "value" {
		test.Fatal("Get failed")
	}

	if v, _ := db.Get([]byte("key")); string(*v) != "value2" {
		test.Fatal("Get failed")
	}

}

func Test_InsertGet(t *testing.T) {
	fname := tempfile()
	db, err := Open(fname, 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	ins := func(key string, val []byte) {
		txn := db.Txn()
		_, update := txn.Insert([]byte(key), []byte(val))
		if update == true {
			t.Fatal("Insert failed")
		}
		db.Commit(txn)
	}

	del := func(key string) {
		txn := db.Txn()
		deleted := txn.Delete([]byte(key))
		if deleted != true {
			t.Fatal("Delete failed")
		}
		db.Commit(txn)
	}

	rand.Seed(0)

	keys := make([]string, 0)
	values := make([][]byte, 0)

	for i := 0; i < 500000; i++ {
		k := RandStringBytesMaskImprSrc(64)
		v := []byte(RandStringBytesMaskImprSrc(120))
		keys = append(keys, k)
		values = append(values, v)
	}

	for i, k := range keys {
		ins(k, values[i])
		//		println("Insert Nodes:", GetNodeCount(), "------", k)
		//		println("------ Tree ------ Root at:", db.header.root)
		//		db.header.root.getNode(db.allocator).printTree(db.allocator, 0)
	}

	println("Nodes:", GetNodeCount())

	for i, k := range keys {
		v := values[i]
		dv, found := db.Get([]byte(k))
		if found == false || string(*dv) != string(v) {
			t.Fatal("Failed", k)
		}

	}

	db.Close()

	db, err = Open(fname, 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to reopen db")
	}

	for i, k := range keys {
		v := values[i]
		dv, found := db.Get([]byte(k))
		if found == false || string(*dv) != string(v) {
			t.Fatalf("Failed %d\n %s\n %s\n (%v)\n", i, dv, string(v), found)
		}
		i++
	}

	for _, k := range keys {
		del(k)
	}
}

func Test_Tables(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Phone struct {
		Id    uint64
		Name  string
		Phone string
	}

	txn := db.Txn()
	txn.CreateTable("PhoneBook")
	txn.CreateIndex(IndexField{
		table: "PhoneBook",
		field: "Phone",
	})

	p1 := Phone{
		Id:    0,
		Name:  "Harry",
		Phone: "555-3456",
	}

	if err := txn.InsertObj("PhoneBook", p1); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", Phone{
		Id:    2,
		Name:  "Natasa",
		Phone: "555-5433",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", Phone{
		Id:    258,
		Name:  "Aspa",
		Phone: "555-1111",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", Phone{
		Id:    1,
		Name:  "Teo",
		Phone: "555-2222",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if _, f := db.Get([]byte("t_PhoneBook")); f != false {
		t.Fatal("Get failed")
	}

	if _, f := txn.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatal("Commit failed")
	}

	if _, f := db.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	txn = db.Txn()
	iter, err := txn.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p2 Phone
	for iter.Next(&p2) {
		fmt.Printf("%d %s %s\n", p2.Id, p2.Name, p2.Phone)
	}

	iter, err = txn.Select("PhoneBook", "Id", uint64(2))
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	iter.Next(&p2)
	if p2.Id != 2 {
		t.Fatal("Returned wrong row")
	}
	more := iter.Next(&p2)
	if more != false {
		t.Fatal("Returned more the one result")
	}

	// Search with secondary index
	iter, err = txn.Select("PhoneBook", "Phone", "555-2222")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}
	iter.Next(&p2)
	if p2.Id != 1 {
		t.Fatal("Returned wrong row")
	}
	more = iter.Next(&p2)
	if more != false {
		t.Fatal("Returned more the one result")
	}

	// Order by secondary index
	iter, err = txn.Select("PhoneBook", "Phone")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	iter.Next(&p2)
	if p2.Id != 258 {
		t.Fatal("Returned wrong row")
	}

	iter.Next(&p2)
	if p2.Id != 1 {
		t.Fatal("Returned wrong row")
	}
}

func Test_ByteArrayCreation(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	fmt.Printf("Free memory: %d\n", db.allocator.GetFree())
	mm := db.allocator

	bPtr, b, err := newBytes(mm, 16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	if bPtr.Size != 16 {
		t.Fatal("Incorrect array size")
	}

	if len(b) != 16 {
		t.Fatal("Incorrect array size")
	}

	rCount := bPtr.getBytesRefCount(mm)
	if *rCount != 1 {
		t.Fatal("bad ref count")
	}

	*rCount++

	if *bPtr.getBytesRefCount(mm) != 2 {
		t.Fatal("bad ref count")
	}

	b[0] = 1
	b[1] = 2
	b[5] = 3
	b[15] = 0xff

	b2 := bPtr.getBytes(mm)
	if len(b2) != 16 {
		t.Fatal("Incorrect array size")
	}

	if b2[0] != 1 || b2[1] != 2 || b2[5] != 3 || b2[15] != 0xff {
		t.Fatal("Data corruption")
	}

	fmt.Printf("Free memory: %d\n", db.allocator.GetFree())
}

func Test_ByteArrayCloneing(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
	mm := db.allocator

	bPtr, b, err := newBytes(mm, 16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	b[0] = 1
	b[1] = 2
	b[5] = 3
	b[15] = 0xff

	b2Ptr, err := bPtr.cloneBytes(mm)
	if err != nil || b2Ptr == nil {
		t.Fatal("Failed to create byte array")
	}

	b2 := b2Ptr.getBytes(mm)
	if len(b2) != 16 {
		t.Fatal("Incorrect array size")
	}

	if b2[0] != 1 || b2[1] != 2 || b2[5] != 3 || b2[15] != 0xff {
		t.Fatal("Data corruption")
	}

	b2[1] = 0xf

	if b2[1] != 0xf || b[1] != 2 {
		t.Fatal("Data corruption")
	}
}

func Test_ByteArrayRefCounting(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
	mm := db.allocator

	bPtr, b, err := newBytes(mm, 16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	bPtr.Retain(mm)

	if *bPtr.getBytesRefCount(mm) != 2 {
		t.Fatal("Bad ref count")
	}

	b[0] = 1
	b[1] = 2
	b[5] = 3
	b[15] = 0xff

	b2Ptr, err := bPtr.cloneBytes(mm)
	if err != nil || b2Ptr == nil {
		t.Fatal("Failed to create byte array")
	}

	if *b2Ptr.getBytesRefCount(mm) != 1 {
		t.Fatal("Bad ref count")
	}

	free := db.allocator.GetFree()
	b2Ptr.Release(mm)
	if db.allocator.GetFree() <= free {
		t.Fatal("Failed to release")
	}

	free = db.allocator.GetFree()
	bPtr.Release(mm)
	if db.allocator.GetFree() != free {
		t.Fatal("Failed to release")
	}
}

func Test_Iterator(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}
	mm := db.allocator

	t := db.Txn()

	if db.header.root.getNode(mm).refCount != 2 {
		test.Fatal("incorrect refcount")
	}

	t.Insert([]byte("Harry"), []byte("value the big universe dude"))
	t.Insert([]byte("Kalogirou"), []byte("this is a last name"))
	t.Insert([]byte("Anna"), []byte("Easy name"))
	t.Insert([]byte("Alexiou"), []byte("Girl"))

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if v, _ := db.Get([]byte("Kalogirou")); string(*v) != "this is a last name" {
		test.Fatal("Get failed")
	}

	iter := db.Iter()
	iter.SeekPrefix([]byte("A"))

	k, v, end := iter.Next()
	if string(k) != "Alexiou" || string(v) != "Girl" {
		test.Fatal("Get failed")
	}

	k, v, end = iter.Next()
	if string(k) != "Anna" || string(v) != "Easy name" {
		test.Fatal("Get failed")
	}

	k, v, end = iter.Next()
	if string(k) != "" || end != false {
		test.Fatal("Get failed")
	}

	iter = db.Iter()
	k, v, end = iter.Next()
	k, v, end = iter.Next()
	k, v, end = iter.Next()
	if string(k) != "Harry" || string(v) != "value the big universe dude" {
		test.Fatal("Get failed")
	}
}

func tempfile() string {
	f, err := ioutil.TempFile("/tmp", "ebakusdb-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}
