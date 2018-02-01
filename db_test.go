package ebakusdb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var src = rand.NewSource(time.Now().UnixNano())

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

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
}

func Test_Tnx(test *testing.T) {

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.Txn()
	old, update := t.Insert([]byte("key"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed")
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

func Test_Get(test *testing.T) {

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	free := db.allocator.TotalFree

	if db.getNode(db.root).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	t := db.Txn()

	if db.getNode(db.root).refCount != 2 {
		test.Fatal("incorrect refcount")
	}

	_, update := t.Insert([]byte("key"), []byte("value the big shit of the universe dude"))
	if update == true {
		test.Fatal("Insert failed")
	}

	if db.getNode(db.root).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	if db.getNode(t.root).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	/*	if v, _ := db.Get([]byte("key")); string(*v) != "value" {
			test.Fatal("Get failed")
		}
	*/
	db.getNode(db.root).Release(db.allocator)

	fmt.Printf("%d %d (%d)\n", free, db.allocator.TotalFree, free-db.allocator.TotalFree)

}

func Test_Get_KeySubset(test *testing.T) {

	db, err := Open("test.db", 0, nil)
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
	db, err := Open("test.db", 0, nil)
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

	data := make(map[string][]byte)

	for i := 0; i < 50000; i++ {
		k := RandStringBytesMaskImprSrc(64)
		v := []byte(RandStringBytesMaskImprSrc(120))
		data[k] = v
	}

	for k, v := range data {
		ins(k, v)
	}

	for k, v := range data {
		dv, found := db.Get([]byte(k))
		if found == false || string(*dv) != string(v) {
			t.Fatal("Failed")
		}
	}

}

func Test_ByteArrayCreation(t *testing.T) {
	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	fmt.Printf("Free memory: %d\n", db.allocator.TotalFree)
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

	fmt.Printf("Free memory: %d\n", db.allocator.TotalFree)
}

func Test_ByteArrayCloneing(t *testing.T) {
	db, err := Open("test.db", 0, nil)
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
	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
	mm := db.allocator

	bPtr, b, err := newBytes(mm, 16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	bPtr.BytesRetain(mm)

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

	free := db.allocator.TotalFree
	b2Ptr.BytesRelease(mm)
	if db.allocator.TotalFree-free != 16 {
		t.Fatal("Failed to release")
	}

	free = db.allocator.TotalFree
	bPtr.BytesRelease(mm)
	bPtr.BytesRelease(mm)
	if db.allocator.TotalFree-free != 16 {
		t.Fatal("Failed to release")
	}
}
