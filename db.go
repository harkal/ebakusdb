package ebakusdb

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/harkal/ebakusdb/balloc"
)

var (
	ErrFailedToCreateDB = errors.New("Failed to create database")
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

	path string
	file *os.File

	bufferRef  []byte
	buffer     *[0x9000000000]byte
	bufferSize uint64
	header     *header
	allocator  *balloc.BufferAllocator
}

const magic uint32 = 0xff01cf11
const version uint32 = 1

type header struct {
	magic   uint32
	version uint32
	root    Ptr
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if options == nil {
		options = DefaultOptions
	}

	if mode == 0 {
		mode = 0666
	}

	db := &DB{
		readOnly: options.ReadOnly,
	}

	flag := os.O_RDWR
	if db.readOnly {
		flag = os.O_RDONLY
	}

	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		fmt.Println(err)
		db.close()
		return nil, err
	}

	info, err := db.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		db.initNewDBFile()
	}

	info, err = db.file.Stat()
	if err != nil {
		return nil, err
	}
	db.mmap(int(info.Size()))

	headerSize := unsafe.Sizeof(header{})
	db.header = (*header)(unsafe.Pointer(&db.bufferRef[0]))
	if db.header.magic != magic {
		return nil, fmt.Errorf("Not an EbakusDB file")
	}
	if db.header.version != version {
		return nil, fmt.Errorf("Unsupported EbakusDB file version")
	}

	allocator, err := balloc.NewBufferAllocator(db.bufferRef, uint64(headerSize))
	if err != nil {
		return nil, err
	}
	allocator.Grow = func(size uint64) error {
		return db.Grow(size)
	}
	db.allocator = allocator

	db.init()

	return db, nil
}

func (db *DB) init() error {
	if db.header.root.isNull() {
		root, _, err := newNode(db.allocator)
		if err != nil {
			return err
		}

		db.header.root = *root
	}

	return nil
}

func (db *DB) initNewDBFile() error {
	var h *header
	buf := make([]byte, unsafe.Sizeof(*h))
	h = (*header)(unsafe.Pointer(&buf[0]))
	h.magic = magic
	h.version = version

	count, err := db.file.Write(buf)
	if count != int(unsafe.Sizeof(*h)) {
		return ErrFailedToCreateDB
	}

	db.Grow(16 * 1024 * 1024)

	return err
}

func (db *DB) Grow(size uint64) error {
	if err := db.file.Truncate(int64(size)); err != nil {
		return fmt.Errorf("file resize error: %s", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("file sync error: %s", err)
	}
	db.bufferSize = size
	return nil
}

func (db *DB) close() error {
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

func decodeKey(key []byte) []byte {
	if len(key)&1 == 1 {
		return nil
	}
	ret := make([]byte, len(key)/2)

	j := 0
	for i := 0; i < len(key)/2; i++ {
		k := key[j]
		j++
		k |= key[j] << 4
		j++
		ret[i] = k
	}
	return ret
}

// Txn starts a new transaction that can be used to mutate the tree
func (db *DB) Txn() *Txn {
	txn := &Txn{
		db:   db,
		root: db.header.root,
	}
	txn.root.getNode(db.allocator).Retain()
	return txn
}

func (db *DB) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return db.header.root.getNode(db.allocator).Get(db, k)
}

func (db *DB) Iter() *Iterator {
	iter := db.header.root.getNode(db.allocator).Iterator(db.allocator)
	return iter
}
