package ebakusdb

import (
	"testing"
)

func Test_Open(t *testing.T) {

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
}

func Test_Tnx(t *testing.T) {

	db, err := Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	db.Txn()
}

func Test_ByteArray(t *testing.T) {
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
