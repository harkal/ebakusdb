package ebakusdb

import (
	"os"

	"github.com/harkal/ebakusdb/balloc"
)

type Options struct {
	// Open database in read-only mode.
	ReadOnly bool
}

// DefaultOptions for the DB
var DefaultOptions = &Options{
	ReadOnly: false,
}

type DB struct {
	readOnly bool

	allocator *balloc.BufferAllocator

	root *Ptr
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if options == nil {
		options = DefaultOptions
	}

	db := &DB{
		readOnly: options.ReadOnly,
	}

	buffer := make([]byte, 1024*1024*1024)

	allocator, err := balloc.NewBufferAllocator(buffer)
	if err != nil {
		return nil, err
	}

	db.allocator = allocator

	db.init()

	return db, nil
}

func (db *DB) init() error {
	root, _, err := newNode(db.allocator)
	if err != nil {
		return err
	}

	db.root = root

	return nil
}

func longestPrefix(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func encodeKey(key []byte) []byte {
	ret := make([]byte, len(key)*2)
	i := 0
	for _, k := range key {
		ret[i] = k & 0xf
		i++
		ret[i] = k >> 4
		i++
	}
	return ret
}

// Txn starts a new transaction that can be used to mutate the tree
func (db *DB) Txn() *Txn {
	txn := &Txn{
		db:   db,
		root: db.root,
	}
	txn.root.getNode(db.allocator).Retain()
	return txn
}

func (db *DB) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return db.root.getNode(db.allocator).Get(db, k)
}
