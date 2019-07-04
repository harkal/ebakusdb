package ebakusdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
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

func Test_Snap(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.GetRootSnapshot()
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
		test.Fatalf("Get failed (got %v)", v)
	}

	db.SetRootSnapshot(t)
	t.Release()

	if v, _ := db.Get([]byte("key")); string(*v) != "va" {
		test.Fatalf("Get failed (got %v)", v)
	}

	if v, _ := db.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %v)", v)
	}

	t = db.GetRootSnapshot()
	old, update = t.Insert([]byte("harry"), []byte("Kal"))
	if update == false {
		test.Fatal("Insert failed")
	}

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %v)", v)
	}

	db.SetRootSnapshot(t)
	t.Release()

	// Change should not be visible outside the transaction
	if v, _ := db.Get([]byte("harry")); string(*v) != "Kal" {
		test.Fatalf("Get failed (got %v)", v)
	}
}

func Test_SnapshotTnx(test *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.GetRootSnapshot()
	_, update := t.Insert([]byte("key"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed value already there")
	}

	_, update = t.Insert([]byte("harry"), []byte("kalogirou"))
	if update == true {
		test.Fatal("Update failed")
	}

	db.SetRootSnapshot(t)
	t.Release()

	snapshot := db.GetRootSnapshot()

	t = db.GetRootSnapshot()
	_, update = t.Insert([]byte("harry"), []byte("Kal"))
	if update == false {
		test.Fatal("Insert failed")
	}

	if v, _ := db.Get([]byte("harry")); v == nil || string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %v)", v)
	}

	db.SetRootSnapshot(t)
	t.Release()

	tnx := snapshot

	if v, _ := tnx.Get([]byte("key")); string(*v) != "value" {
		test.Fatalf("Get failed (got '%s')", string(*v))
	}

	// Change should not be visible on this snapshot
	if v, _ := tnx.Get([]byte("harry")); string(*v) != "kalogirou" {
		test.Fatalf("Get failed (got %v)", v)
	}

	// But should be visible here
	if v, _ := db.Get([]byte("harry")); string(*v) != "Kal" {
		test.Fatalf("Get failed (got %v)", v)
	}

	tnx.Release()
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

	t := db.GetRootSnapshot()

	if db.header.root.getNode(mm).refCount != 2 {
		test.Fatal("incorrect refcount")
	}

	_, update := t.Insert([]byte("key"), []byte("value the big universe dude"))
	if update == true {
		test.Fatal("Insert failed")
	}

	var deleted bool

	db.SetRootSnapshot(t)
	t.Release()

	if db.header.root.getNode(mm).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	if db.header.root.getNode(mm).refCount != 1 {
		test.Fatal("incorrect refcount")
	}

	t = db.GetRootSnapshot()
	_, update = t.Insert([]byte("harry"), []byte("NEW VALUE"))
	if update == true {
		test.Fatal("Insert failed")
	}

	db.SetRootSnapshot(t)
	t.Release()

	t = db.GetRootSnapshot()
	_, update = t.Insert([]byte("bobby"), []byte("NEW"))
	if update == true {
		test.Fatal("Insert failed")
	}

	deleted = t.Delete([]byte("key"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	deleted = t.Delete([]byte("harry"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	deleted = t.Delete([]byte("bobby"))
	if deleted != true {
		test.Fatal("Delete failed")
	}

	db.SetRootSnapshot(t)
	t.Release()

	//fmt.Printf("%d %d (%d)\n", free, db.allocator.GetFree(), int(free)-int(db.allocator.GetFree()))
}

func Test_Get_KeySubset(test *testing.T) {

	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		test.Fatal("Failed to open db")
	}

	t := db.GetRootSnapshot()
	_, update := t.Insert([]byte("key_long"), []byte("value"))
	if update == true {
		test.Fatal("Insert failed")
	}
	_, update = t.Insert([]byte("key"), []byte("value2"))
	if update == true {
		test.Fatal("Insert failed")
	}
	db.SetRootSnapshot(t)
	t.Release()

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
		txn := db.GetRootSnapshot()
		_, update := txn.Insert([]byte(key), []byte(val))
		if update == true {
			t.Fatal("Insert failed")
		}
		db.SetRootSnapshot(txn)
		txn.Release()
	}

	del := func(key string) {
		txn := db.GetRootSnapshot()
		deleted := txn.Delete([]byte(key))
		if deleted != true {
			t.Fatal("Delete failed")
		}
		db.SetRootSnapshot(txn)
		txn.Release()
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
			t.Fatalf("Failed %d\n %v\n %s\n (%v)\n", i, dv, string(v), found)
		}
		i++
	}

	for _, k := range keys {
		del(k)
	}
}

func Test_LargeValue(t *testing.T) {
	fname := tempfile()
	db, err := Open(fname, 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db")
	}

	s := db.GetRootSnapshot()

	key := []byte("key")
	value := make([]byte, 1024)
	value[0] = 30

	s.Insert(key, value)

	v, f := s.Get(key)
	if f != true {
		t.Fatalf("Failed to find key")
	}

	if !bytes.Equal(*v, value) {
		t.Fatalf("Failed to get proper value")
	}

	s.Release()
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

	txn := db.GetRootSnapshot()
	txn.CreateTable("PhoneBook", &Phone{})
	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Phone",
	})

	p1 := Phone{
		Id:    0,
		Name:  "Harry",
		Phone: "555-3456",
	}

	if err := txn.InsertObj("PhoneBook", &p1); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    2,
		Name:  "Natasa",
		Phone: "555-5433",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    258,
		Name:  "Aspa",
		Phone: "555-1111",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
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

	db.SetRootSnapshot(txn)
	txn.Release()

	if _, f := db.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	txn = db.GetRootSnapshot()
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

	txn.Release()
}

func Test_QueryTokenizer(t *testing.T) {
	// WHERE query
	tokenizer := NewTokenizer([]string{"<", ">", "=", "==", "<=", ">=", "LIKE"})

	// "Name LIKE a b  "
	expected := [][]byte{[]byte{78, 97, 109, 101}, []byte{76, 73, 75, 69}, []byte{97, 32, 98, 0, 240}}
	parts := tokenizer.Tokenize([]byte{78, 97, 109, 101, 32, 76, 73, 75, 69, 32, 97, 32, 98, 0, 240})
	if len(parts) != 3 || !reflect.DeepEqual(parts, expected) {
		t.Fatal("Wrong output", parts)
	}

	// "Name>a b  "
	expected = [][]byte{[]byte{78, 97, 109, 101}, []byte{62}, []byte{97, 32, 98, 0, 240}}
	parts = tokenizer.Tokenize([]byte{78, 97, 109, 101, 62, 97, 32, 98, 0, 240})
	if len(parts) != 3 || !reflect.DeepEqual(parts, expected) {
		t.Fatal("Wrong output", parts)
	}

	// "Name> a b  "
	expected = [][]byte{[]byte{78, 97, 109, 101}, []byte{62}, []byte{97, 32, 98, 0, 240}}
	parts = tokenizer.Tokenize([]byte{78, 97, 109, 101, 62, 32, 97, 32, 98, 0, 240})
	if len(parts) != 3 || !reflect.DeepEqual(parts, expected) {
		t.Fatal("Wrong output", parts)
	}

	// "Name LIKE a LIKE b  "
	expected = [][]byte{[]byte{78, 97, 109, 101}, []byte{76, 73, 75, 69}, []byte{97, 32, 76, 73, 75, 69, 32, 98, 0, 240}}
	parts = tokenizer.Tokenize([]byte{78, 97, 109, 101, 32, 76, 73, 75, 69, 32, 97, 32, 76, 73, 75, 69, 32, 98, 0, 240})
	if len(parts) != 3 || !reflect.DeepEqual(parts, expected) {
		t.Fatal("Wrong output", parts)
	}

	// ORDER query
	tokenizer = NewTokenizer([]string{"ASC", "DESC"})

	// "Name DESC"
	expected = [][]byte{[]byte{78, 97, 109, 101}, []byte{68, 69, 83, 67}}
	parts = tokenizer.Tokenize([]byte{78, 97, 109, 101, 32, 68, 69, 83, 67})
	if len(parts) != 2 || !reflect.DeepEqual(parts, expected) {
		t.Fatal("Wrong output", parts)
	}
}

func Test_TableWhereParser(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	txn := db.GetRootSnapshot()

	query := txn.WhereParser([]byte(""))
	if query != nil {
		t.Fatal("Wrong where query", query)
	}


	query = txn.WhereParser([]byte("Name = Harry"))
	if query.Condition != Equal {
		t.Fatal("Wrong where query", query)
	}

	query = txn.WhereParser([]byte("Name <= Harry"))
	if query.Condition != SmallerOrEqual {
		t.Fatal("Wrong where query", query)
	}
}

func Test_TableOrderParser(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	txn := db.GetRootSnapshot()

	query := txn.OrderParser([]byte("Name"))
	if query.Order != ASC {
		t.Fatal("Wrong order query", query)
	}

	query = txn.OrderParser([]byte("Name DESC"))
	if query.Order != DESC {
		t.Fatal("Wrong order query", query)
	}
}

func Test_TablesSelect(t *testing.T) {
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

	txn := db.GetRootSnapshot()
	txn.CreateTable("PhoneBook", &Phone{})
	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Name",
	})
	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Phone",
	})

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    0,
		Name:  "Harry Kalogirou",
		Phone: "555-2222",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    1,
		Name:  "Harry Who",
		Phone: "555-1111",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    2,
		Name:  "Chris",
		Phone: "555-1333",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := txn.Select("PhoneBook", "Name LIKE Harry", "Phone ASC")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p Phone
	for iter.Next(&p) {
		fmt.Printf("%d %s %s\n", p.Id, p.Name, p.Phone)
	}

	found := iter.Next(&p)
	if !found || p.Id != 1 {
		t.Fatal("Returned wrong row", &p, found)
	}

	found = iter.Next(&p)
	if !found || p.Id != 0 {
		t.Fatal("Returned wrong row", &p, found)
	}

	found = iter.Next(&p)
	if found {
		t.Fatal("Shouldn't return more rows", &p, found)
	}

	txn.Release()
}

func Test_TableOrdering(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    [4]byte
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    [4]byte{1, 2, 3, 4},
		Stake: 1000,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Id", [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness

	iter.Next(&w)
	if w.Stake != 1000 {
		t.Fatal("Returned wrong row")
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    [4]byte{1, 2, 3, 5},
		Stake: 2000,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    [4]byte{1, 2, 3, 6},
		Stake: 100,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    [4]byte{1, 2, 2, 5},
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err = snap.Select(WitnessesTable, "Stake")

	lastStake := uint64(3000)
	for iter.Prev(&w) {
		if w.Stake >= lastStake {
			t.Fatal("Improper ordering")
		}
		lastStake = w.Stake
	}
}

func Test_TableDuplicates(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    [4]byte
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    [4]byte{1, 2, 3, 4},
		Stake: 1000,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Id", [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness

	iter.Next(&w)
	if w.Stake != 1000 {
		t.Fatal("Returned wrong row")
	}

	if iter.Next(&w) != false {
		t.Fatal("Next row found", &w)
	}

	if iter.Prev(&w) != false {
		t.Fatal("Prev row found", &w)
	}

	// force an update, and test that it doesn't duplicate the entry
	w.Stake = 1001

	if err := snap.InsertObj(WitnessesTable, &w); err != nil {
		t.Fatal("Failed to update row error:", err)
	}

	iter, err = snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	iter.Next(&w)
	if iter.Next(&w) == true {
		t.Fatal("Has next", &w)
	}

	iter.Prev(&w)
	if iter.Prev(&w) == true {
		t.Fatal("Has prev", &w)
	}
}

func Test_TablesInsertIndexes(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    uint64
		Stake uint64
		Extra uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Extra",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 2,
		Extra: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	reflectType := reflect.StructOf([]reflect.StructField{
		reflect.StructField{
			Name: "Id",
			Type: reflect.TypeOf(uint64(0)),
		},
		reflect.StructField{
			Name: "Stake",
			Type: reflect.TypeOf(uint64(0)),
		},
		reflect.StructField{
			Name: "Extra",
			Type: reflect.TypeOf(uint64(0)),
		},
	})
	reflectInstance := reflect.New(reflectType)
	reflectInstance.Elem().FieldByName("Id").SetUint(1)
	reflectInstance.Elem().FieldByName("Stake").SetUint(4)
	reflectInstance.Elem().FieldByName("Extra").SetUint(4)

	reflectInterface := reflectInstance.Interface()

	if err := snap.InsertObj(WitnessesTable, reflectInterface); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 5,
		Extra: 5,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}
}

func Test_TablesDeleteIndexes(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    uint64
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 200,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    2,
		Stake: 100,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness

	iter.Next(&w)
	if w.Id != 2 {
		t.Fatal("Returned wrong row", &w)
	}

	if err := snap.DeleteObj(WitnessesTable, uint64(2)); err != nil {
		t.Fatal("Failed to delete row error:", err)
	}

	iter, err = snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	found := iter.Next(&w)
	if !found || w.Id != 1 {
		t.Fatal("Returned wrong row", &w, found)
	}

	if iter.Next(&w) {
		t.Fatal("Shouldn't find second row")
	}
}

func Test_TablesUpdateIndexes(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Phone struct {
		Id    uint64
		Name  string
		Code  string
		Phone string
	}

	txn := db.GetRootSnapshot()
	txn.CreateTable("PhoneBook", &Phone{})

	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Phone",
	})

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    1,
		Name:  "Harry",
		Code:  "+30",
		Phone: "555-3456",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    2,
		Name:  "Natasa",
		Code:  "+31",
		Phone: "1",
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := txn.InsertObj("PhoneBook", &Phone{
		Id:    2,
		Name:  "Natasa",
		Code:  "+41",
		Phone: "2",
	}); err != nil {
		t.Fatal("Failed to update row error:", err)
	}

	iter, err := txn.Select("PhoneBook", "Phone")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var p3 Phone
	if found := iter.Next(&p3); !found || p3.Name != "Natasa" {
		t.Fatal("Returned wrong row", p3, found)
	}
}

func Test_TablesInsertIndexesWithSameValue(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    uint64
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    2,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness
	ws := make(map[uint64]bool)

	for iter.Next(&w) {
		ws[w.Id] = true
	}

	if len(ws) != 2 {
		t.Fatal("Wrong number of entries, expected: 2, got:", len(ws))
	}

	if !ws[1] || !ws[2] {
		t.Fatal("Wrong entries", ws)
	}
}

func Test_TablesSelectReturnsWrongRows(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Delegation struct {
		Id [2]byte
	}

	const DelegationsTable string = "Delegations"

	db.CreateTable(DelegationsTable, &Delegation{})
	snap := db.GetRootSnapshot()

	p1d1 := [2]byte{1, 1}
	p1d2 := [2]byte{1, 2}
	p2d1 := [2]byte{20, 1}

	fmt.Println("------ Insert p1d")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1d1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1d2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Insert p2d")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2d1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Delete 1 for p2d")

	if err := snap.DeleteObj(DelegationsTable, p2d1); err != nil {
		fmt.Println("err", err)
	}

	fmt.Println("------ Select 0 for p2d")

	iter, err := snap.Select(DelegationsTable, "Id", p2d1[:1] /* 20 */)
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var d Delegation
	for iter.Next(&d) {
		fmt.Println("  ", d)
		t.Fatal("Shouldn't find row", d)
	}
}

func Test_InsertLookupPrefixAfterMerge(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Delegation struct {
		Id [2]byte
	}

	const DelegationsTable string = "Delegations"

	db.CreateTable(DelegationsTable, &Delegation{})
	snap := db.GetRootSnapshot()

	p1 := [2]byte{1, 20}
	p2 := [2]byte{20, 1}

	fmt.Println("------ Insert 2")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}
	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Delete p1")
	if err := snap.DeleteObj(DelegationsTable, p1); err != nil {
		fmt.Println("err", err)
	}

	fmt.Println("------ Update p2")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Select ALL")

	var d Delegation

	iter, err := snap.Select(DelegationsTable, "Id")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	i := 0
	for iter.Next(&d) {
		fmt.Println(d)
		i++
		if i > 1 || d.Id != p2 {
			t.Fatal("Found wrong rows", i, d)
		}
	}
}

func Test_InsertLookupPrefixAfterMergeOnParentTableNode(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Delegation struct {
		Id [3]byte
	}

	const DelegationsTable string = "Delegations"

	db.CreateTable(DelegationsTable, &Delegation{})
	snap := db.GetRootSnapshot()

	p1 := [3]byte{1, 20, 0}
	p2 := [3]byte{20, 1, 0}
	p3 := [3]byte{20, 1, 40}

	fmt.Println("------ Insert 2")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}
	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Delete p1")
	if err := snap.DeleteObj(DelegationsTable, p1); err != nil {
		fmt.Println("err", err)
	}

	fmt.Println("------ Update p2")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Insert p3")
	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p3,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Check prefix")

	fmt.Print("\n\n")

	mm := db.allocator
	tPtrMarshaled, _ := snap.Get([]byte("t_" + DelegationsTable))
	var tbl Table
	db.decode(*tPtrMarshaled, &tbl)
	tNode := tbl.Node.getNode(mm)

	tNodePrefix := tNode.prefixPtr.getBytes(mm)
	if len(tNodePrefix) != 0 {
		t.Fatal("Parent node has prefix set:", tNodePrefix)
	}

	childPtr := tNode.getFirstChild()
	child := childPtr.getNode(mm)

	childPrefix := child.prefixPtr.getBytes(mm)
	expectedPrefix := encodeKey(p3[:2])
	if !bytes.Equal(childPrefix, expectedPrefix) {
		t.Fatal("Child has wrong prefix:", childPrefix, "expectedPrefix:", expectedPrefix)
	}

	p2NodePrefix := encodeKey(p2[2:])
	p2Node := child.edges[p2NodePrefix[0]].getNode(mm)
	p2NodeKey := p2Node.keyPtr.getBytes(mm)
	if !bytes.Equal(p2NodeKey, encodeKey(p2[:])) {
		t.Fatal("p2 node is wrong:", p2NodeKey)
	}

	p3NodePrefix := encodeKey(p3[2:])
	p3Node := child.edges[p3NodePrefix[0]].getNode(mm)
	p3NodeKey := p3Node.keyPtr.getBytes(mm)
	if !bytes.Equal(p3NodeKey, encodeKey(p3[:])) {
		t.Fatal("p3 node is wrong:", p3NodeKey)
	}

	fmt.Println("------ Select ALL")

	var d Delegation

	iter, err := snap.Select(DelegationsTable, "Id")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	if found := iter.Next(&d); !found || d.Id != p2 {
		t.Fatal("Found wrong row (expected p2)", found, d)
	}

	if found := iter.Next(&d); !found || d.Id != p3 {
		t.Fatal("Found wrong row (expected p3)", found, d)
	}
}

func Test_InsertLookupPrefixAfterMergeOnParentTableNodeForEmptyIdValue(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Delegation struct {
		Id []byte
	}

	const DelegationsTable string = "Delegations"

	db.CreateTable(DelegationsTable, &Delegation{})
	snap := db.GetRootSnapshot()

	p1 := []byte{20}
	p2 := []byte{}
	p3 := []byte{40, 0}

	fmt.Println("------ Insert 2")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}
	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Select p2 (has empty Id)")

	var d Delegation

	iter, err := snap.Select(DelegationsTable, "Id", p2)
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	if found := iter.Next(&d); !found {
		t.Fatal("Haven't found row with empty Id", found, d)
	}

	fmt.Println("------ Delete p2 (has empty Id)")

	if err := snap.DeleteObj(DelegationsTable, p2); err != nil {
		fmt.Println("err", err)
	}

	fmt.Println("------ Check prefix p1")

	mm := db.allocator
	tPtrMarshaled, _ := snap.Get([]byte("t_" + DelegationsTable))
	var tbl Table
	db.decode(*tPtrMarshaled, &tbl)
	tNode := tbl.Node.getNode(mm)

	tNodePrefix := tNode.prefixPtr.getBytes(mm)
	if len(tNodePrefix) != 0 {
		t.Fatal("Parent node has prefix set:", tNodePrefix)
	}

	p1NodePrefix := encodeKey(p1)
	p1Node := tNode.edges[p1NodePrefix[0]].getNode(mm)
	p1NodeKey := p1Node.keyPtr.getBytes(mm)
	if !bytes.Equal(p1NodeKey, encodeKey(p1[:])) {
		t.Fatal("p1 node is wrong:", p1NodeKey)
	}

	fmt.Println("------ Insert p3")

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p3,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	fmt.Println("------ Check prefix p3")

	tPtrMarshaled, _ = snap.Get([]byte("t_" + DelegationsTable))
	db.decode(*tPtrMarshaled, &tbl)
	tNode = tbl.Node.getNode(mm)

	p3NodePrefix := encodeKey(p3)
	p3Node := tNode.edges[p3NodePrefix[0]].getNode(mm)
	p3NodeKey := p3Node.keyPtr.getBytes(mm)
	if !bytes.Equal(p3NodeKey, encodeKey(p3[:])) {
		t.Fatal("p3 node is wrong:", p3NodeKey)
	}

	fmt.Println("------ Select ALL")

	iter, err = snap.Select(DelegationsTable, "Id")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	if found := iter.Next(&d); !found || !bytes.Equal(d.Id, p1) {
		t.Fatal("Found wrong row (expected p2)", found, d)
	}

	if found := iter.Next(&d); !found || !bytes.Equal(d.Id, p3) {
		t.Fatal("Found wrong row (expected p3)", found, d)
	}

	if found := iter.Next(&d); found {
		t.Fatal("Found more rows", found, d)
	}
}

func Test_DeleteLookupPrefixAfterMerge(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Delegation struct {
		Id [2]byte
	}

	const DelegationsTable string = "Delegations"

	db.CreateTable(DelegationsTable, &Delegation{})
	snap := db.GetRootSnapshot()

	p1 := [2]byte{1, 20}
	p2 := [2]byte{20, 1}

	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p1,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}
	if err := snap.InsertObj(DelegationsTable, &Delegation{
		Id: p2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.DeleteObj(DelegationsTable, p1); err != nil {
		fmt.Println("err", err)
	}
	if err := snap.DeleteObj(DelegationsTable, p2); err != nil {
		fmt.Println("err", err)
	}

	var d Delegation

	iter, err := snap.Select(DelegationsTable, "Id")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	if iter.Next(&d) {
		t.Fatal("Found rows", d)
	}
}

func Test_TablesDeleteIndexesWithSameValue(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    uint64
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    2,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.DeleteObj(WitnessesTable, uint64(2)); err != nil {
		t.Fatal("Failed to delete row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness

	found := iter.Next(&w)
	if !found || w.Id != 1 {
		t.Fatal("Returned wrong row", w, found)
	}

	if iter.Next(&w) {
		t.Fatal("Shouldn't return row", w)
	}
}

func Test_TablesUpdateIndexesWithSameValue(t *testing.T) {
	db, err := Open(tempfile(), 0, nil)
	defer os.Remove(db.GetPath())
	if err != nil || db == nil {
		t.Fatal("Failed to open db", err)
	}

	type Witness struct {
		Id    uint64
		Stake uint64
	}

	const WitnessesTable string = "Witnesses"

	db.CreateTable(WitnessesTable, &Witness{})
	db.CreateIndex(IndexField{
		Table: WitnessesTable,
		Field: "Stake",
	})

	snap := db.GetRootSnapshot()

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    2,
		Stake: 2,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	// Update Witness #1
	if err := snap.InsertObj(WitnessesTable, &Witness{
		Id:    1,
		Stake: 3,
	}); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := snap.Select(WitnessesTable, "Stake")
	if err != nil {
		t.Fatal("Failed to create iterator error:", err)
	}

	var w Witness
	ws := make(map[uint64]uint64)

	for iter.Next(&w) {
		ws[w.Id] = w.Stake
	}

	if len(ws) != 2 {
		t.Fatal("Wrong number of entries, expected: 2, got:", len(ws))
	}

	if ws[1] != 3 || ws[2] != 2 {
		t.Fatal("Wrong entries", ws)
	}
}

func Test_SnapshotResetTo(t *testing.T) {
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

	txn := db.GetRootSnapshot()
	// defer txn.Release()

	txn.CreateTable("PhoneBook", &Phone{})
	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Phone",
	})

	p1 := Phone{
		Id:    0,
		Name:  "Harry",
		Phone: "555-3456",
	}

	if err := txn.InsertObj("PhoneBook", &p1); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	if _, f := db.Get([]byte("t_PhoneBook")); f != false {
		t.Fatal("Get failed")
	}

	if _, f := txn.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	// similar to genesis block
	db.SetRootSnapshot(txn)
	txn.Release()

	if _, f := db.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	// txn2 := txn.Snapshot()
	txn2 := db.GetRootSnapshot()
	txn2OrigSnap := txn2.Snapshot()
	// defer txn2OrigSnap.Release()

	p2 := Phone{
		Id:    0,
		Name:  "Harry who?",
		Phone: "555-3456",
	}

	if err := txn2.InsertObj("PhoneBook", &p2); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err := txn2.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p3 Phone
	iter.Next(&p3)
	if p3.Name != "Harry who?" {
		t.Fatal("Returned wrong row")
	}

	txn2.ResetTo(txn2OrigSnap)
	txn2OrigSnap.Release()

	iter, err = txn2.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p4 Phone
	iter.Next(&p4)
	if p4.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}

	// db.SetRootSnapshot(txn2)
	txn2.Release()

	if _, f := db.Get([]byte("t_PhoneBook")); f != true {
		t.Fatal("Get failed")
	}

	txn3 := db.GetRootSnapshot()

	iter, err = txn3.Select("PhoneBook", "Phone")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p5 Phone
	if iter.Next(&p5) == false {
		t.Fatal("No row found")
	}
	if p5.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}
}

func Test_SnapshotResetToSelectNoEntries(t *testing.T) {
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

	txn := db.GetRootSnapshot()

	txn.CreateTable("PhoneBook", &Phone{})

	p1 := Phone{
		Id:    0,
		Name:  "Harry",
		Phone: "555-3456",
	}

	if err := txn.InsertObj("PhoneBook", &p1); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	snapForResetTo := txn.Snapshot()

	iter, err := txn.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p2 Phone
	if found := iter.Next(&p2); !found || p2.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}

	p2.Name = "Harry who?"

	if err := txn.InsertObj("PhoneBook", &p2); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	iter, err = snapForResetTo.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p3 Phone
	if iter.Next(&p3) == false {
		t.Fatal("No row found")
	}
	println(p3.Id, " ", p3.Name, " ", p3.Phone)
	if p3.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}
}

func Test_SnapshotResetToSelectIndexNoEntries(t *testing.T) {
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

	txn := db.GetRootSnapshot()

	txn.CreateTable("PhoneBook", &Phone{})
	txn.CreateIndex(IndexField{
		Table: "PhoneBook",
		Field: "Phone",
	})

	p1 := Phone{
		Id:    0,
		Name:  "Harry",
		Phone: "555-3456",
	}

	if err := txn.InsertObj("PhoneBook", &p1); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	snapForResetTo := txn.Snapshot()

	iter, err := txn.Select("PhoneBook", "Phone")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p2 Phone
	if iter.Next(&p2) != true {
		t.Fatal("No row found")
	}

	if p2.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}

	p2.Name = "Harry who?"

	if err := txn.InsertObj("PhoneBook", &p2); err != nil {
		t.Fatal("Failed to insert row error:", err)
	}

	//
	// Test return row without index sort
	//
	iter, err = snapForResetTo.Select("PhoneBook")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	var p3 Phone
	if iter.Next(&p3) == false {
		t.Fatal("No row found")
	}
	if p3.Name != "Harry" {
		t.Fatal("Returned wrong row")
	}

	//
	// Test return row with index sort
	//
	iter, err = snapForResetTo.Select("PhoneBook", "Phone")
	if err != nil {
		t.Fatal("Failed to create iterator")
	}

	if iter.Next(&p3) == false {
		t.Fatal("No row found")
	}
	if p3.Name != "Harry" {
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

	t := db.GetRootSnapshot()

	if db.header.root.getNode(mm).refCount != 2 {
		test.Fatal("incorrect refcount")
	}

	t.Insert([]byte("Harry"), []byte("value the big universe dude"))
	t.Insert([]byte("Kalogirou"), []byte("this is a last name"))
	t.Insert([]byte("Anna"), []byte("Easy name"))
	t.Insert([]byte("Alexiou"), []byte("Girl"))

	db.SetRootSnapshot(t)
	t.Release()

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

	iter = db.Iter()
	iter.SeekPrefix([]byte("G"))
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
