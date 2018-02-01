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
	old, update := t.Insert([]byte("key"), "value")
	if update == true {
		test.Fatal("Insert failed")
	}
	fmt.Println("old:", old)
	old, update = t.Insert([]byte("key"), "va")
	if update == false || old != "value" {
		test.Fatal("Update failed")
	}
	fmt.Println("old:", old)

	old, update = t.Insert([]byte("harry"), "kalogirou")
	if update == true {
		test.Fatal("Update failed")
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if v, _ := db.Get([]byte("key")); v != "va" {
		test.Fatalf("Get failed (got %s)", v)
	}

	if v, _ := db.Get([]byte("harry")); v != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	t = db.Txn()
	old, update = t.Insert([]byte("harry"), "Kal")
	if update == false {
		test.Fatal("Insert failed")
	}

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); v != "kalogirou" {
		test.Fatalf("Get failed (got %s)", v)
	}

	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); v != "Kal" {
		test.Fatalf("Get failed (got %s)", v)
	}
}

func Test_Get(test *testing.T) {

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.Txn()
	_, update := t.Insert([]byte("key"), "value")
	if update == true {
		test.Fatal("Insert failed")
	}
	err = db.Commit(t)
	if err != nil {
		test.Fatal("Commit failed")
	}

	if v, _ := db.Get([]byte("key")); v != "value" {
		test.Fatal("Get failed")
	}

}

func Test_InsertGet(t *testing.T) {
	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	ins := func(key, val string) {
		txn := db.Txn()
		_, update := txn.Insert([]byte(key), val)
		if update == true {
			t.Fatal("Insert failed")
		}
		db.Commit(txn)
	}

	data := make(map[string]string)

	for i := 0; i < 50000; i++ {
		k := RandStringBytesMaskImprSrc(64)
		v := RandStringBytesMaskImprSrc(120)
		data[k] = v
	}

	for k, v := range data {
		ins(k, v)
	}

	for k, v := range data {
		dv, found := db.Get([]byte(k))
		if found == false || dv != v {
			t.Fatal("Failed")
		}
	}

}

func Test_ByteArrayCreation(t *testing.T) {
	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	bPtr, b, err := db.newBytes(16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	if bPtr.Size != 16 {
		t.Fatal("Incorrect array size")
	}

	if len(b) != 16 {
		t.Fatal("Incorrect array size")
	}

	b[0] = 1
	b[1] = 2
	b[5] = 3
	b[15] = 0xff

	b2 := db.getBytes(bPtr)
	if len(b2) != 16 {
		t.Fatal("Incorrect array size")
	}

	if b2[0] != 1 || b2[1] != 2 || b2[5] != 3 || b2[15] != 0xff {
		t.Fatal("Data corruption")
	}
}

func Test_ByteArrayCloneing(t *testing.T) {
	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	bPtr, b, err := db.newBytes(16)
	if err != nil || bPtr == nil {
		t.Fatal("Failed to create byte array")
	}

	b[0] = 1
	b[1] = 2
	b[5] = 3
	b[15] = 0xff

	b2Ptr, err := db.cloneBytes(bPtr)
	if err != nil || b2Ptr == nil {
		t.Fatal("Failed to create byte array")
	}

	b2 := db.getBytes(b2Ptr)
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
