package ebakusdb

import (
	"os"
	"time"
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

// Options represents the options for the database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Open database in read-only mode.
	ReadOnly bool
}

// DefaultOptions for the DB
var DefaultOptions = &Options{
	Timeout: 0,
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

	buffer := make([]byte, 512*1024*1024)

	allocator, err := balloc.NewBufferAllocator(buffer)
	if err != nil {
		return nil, err
	}

	db.allocator = allocator

	db.init()

	return db, nil
}

func (db *DB) init() error {
	root, _, err := db.newNode()
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

// Txn starts a new transaction that can be used to mutate the tree
func (db *DB) Txn() *Txn {
	txn := &Txn{
		db:   db,
		root: db.root,
	}
	return txn
}

func (db *DB) Commit(txn *Txn) error {
	newRootPtr := txn.Commit()
	db.getNode(db.root).Release()
	db.root = newRootPtr
	return nil
}

func (db *DB) newNode() (*Ptr, *Node, error) {
	offset, err := db.allocator.Allocate(uint64(unsafe.Sizeof(Node{})))
	if err != nil {
		return nil, nil, err
	}
	p := &Ptr{Offset: offset}
	n := db.getNode(p)
	n.Retain()
	return p, n, nil
}

func (db *DB) getNode(p *Ptr) *Node {
	return (*Node)(db.allocator.GetPtr(p.Offset))
}

func (db *DB) Get(k []byte) (interface{}, bool) {
	return db.getNode(db.root).Get(db, k)
}

func (db *DB) newLeafNode() (*Ptr, *leafNode, error) {
	offset, err := db.allocator.Allocate(uint64(unsafe.Sizeof(leafNode{})))
	if err != nil {
		return nil, nil, err
	}
	p := &Ptr{Offset: offset}
	n := db.getLeafNode(p)
	return p, n, nil
}

func (db *DB) getLeafNode(p *Ptr) *leafNode {
	return (*leafNode)(db.allocator.GetPtr(p.Offset))
}

func (db *DB) newBytes(size uint64) (*ByteArray, []byte, error) {
	offset, err := db.allocator.Allocate(uint64(unsafe.Sizeof(ByteArray{}) + uintptr(size)))
	if err != nil {
		return nil, nil, err
	}
	aPtr := &ByteArray{Offset: offset, Size: size}
	a := db.getBytes(aPtr)
	return aPtr, a, nil
}

func (db *DB) newBytesFromSlice(data []byte) *ByteArray {
	aPtr, a, err := db.newBytes(uint64(len(data)))
	if err != nil {
		panic(err)
	}
	copy(a, data)
	return aPtr
}

func (db *DB) cloneBytes(bPtr *ByteArray) (*ByteArray, error) {
	newBPtr, newB, err := db.newBytes(bPtr.Size)
	if err != nil {
		return nil, err
	}

	old := db.getBytes(bPtr)

	copy(newB, old)

	return newBPtr, nil
}

func (db *DB) getBytes(b *ByteArray) []byte {
	return (*[0x7fffff]byte)(db.allocator.GetPtr(b.Offset + uint64(unsafe.Sizeof(ByteArray{}))))[:b.Size]
}
