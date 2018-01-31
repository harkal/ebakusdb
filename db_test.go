package ebakusdb_test

import (
	"testing"

	"github.com/harkal/ebakusdb"
)

func Test_Open(t *testing.T) {

	db, err := ebakusdb.Open("test.db", 0, nil)
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}
}
