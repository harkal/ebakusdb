package ebakusdb

import (
	"encoding/json"
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

type DBEncoder func(val interface{}) ([]byte, error)
type DBDecoder func(b []byte, val interface{}) error

type DB struct {
	readOnly bool

	path string
	file *os.File

	bufferRef  []byte
	buffer     *[0x9000000000]byte
	bufferSize uint64
	header     *header
	allocator  *balloc.BufferAllocator

	encode DBEncoder
	decode DBDecoder
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
		encode:   json.Marshal,
		decode:   json.Unmarshal,
	}

	flag := os.O_RDWR
	if db.readOnly {
		flag = os.O_RDONLY
	}

	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		fmt.Println(err)
		db.Close()
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

	//db.bufferSize = 8 * 1024 * 1024 * 1024
	//db.bufferRef = make([]byte, db.bufferSize)
	//db.buffer = (*[0x9000000000]byte)(unsafe.Pointer(&db.bufferRef[0]))
	//h := (*header)(unsafe.Pointer(&db.bufferRef[0]))
	//h.magic = magic
	//h.version = version

	headerSize := unsafe.Sizeof(header{})
	db.header = (*header)(unsafe.Pointer(&db.bufferRef[0]))
	if db.header.magic != magic {
		return nil, fmt.Errorf("Not an EbakusDB file")
	}
	if db.header.version != version {
		return nil, fmt.Errorf("Unsupported EbakusDB file version")
	}

	psize := uint16(unsafe.Sizeof(Node{}))
	allocator, err := balloc.NewBufferAllocator(unsafe.Pointer(&db.bufferRef[0]), uint64(len(db.bufferRef)), uint64(headerSize), psize)
	if err != nil {
		return nil, err
	}

	db.allocator = allocator

	db.init()

	//fmt.Printf("Inited EbakusDB with %d MB of storage\n", info.Size()/megaByte)

	return db, nil
}

func (db *DB) SetCustomEncoder(encode DBEncoder, decode DBDecoder) {
	db.encode = encode
	db.decode = decode
}

func (db *DB) GetPath() string {
	return db.path
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

	if err := db.file.Truncate(1 * megaByte); err != nil {
		return fmt.Errorf("file resize error: %s", err)
	}

	return err
}

const megaByte = 1024 * 1024
const gigaByte = 1024 * megaByte

func (db *DB) Grow() error {
	if float32(db.allocator.GetFree()) > float32(db.allocator.GetCapacity())*0.3 {
		return nil
	}

	var newSize = db.allocator.GetCapacity()

	if newSize < gigaByte {
		newSize *= 2
	} else if newSize >= gigaByte {
		newSize += gigaByte
	}

	//fmt.Printf("Will grow to %d MB\n", newSize/megaByte)

	if err := db.munmap(); err != nil {
		return fmt.Errorf("Failed to unmap memory error: %s", err)
	}

	if err := db.file.Truncate(int64(newSize)); err != nil {
		return fmt.Errorf("file resize error: %s", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("file sync error: %s", err)
	}

	if err := db.mmap(int(newSize)); err != nil {
		return fmt.Errorf("Failed to map memory error: %s", err)
	}

	headerSize := unsafe.Sizeof(header{})
	db.header = (*header)(unsafe.Pointer(&db.bufferRef[0]))
	db.allocator.SetBuffer(unsafe.Pointer(&db.buffer[0]), newSize, uint64(headerSize))

	return nil
}

func (db *DB) Close() error {
	if err := db.munmap(); err != nil {
		return fmt.Errorf("Failed to unmap memory error: %s", err)
	}
	if err := db.file.Close(); err != nil {
		return fmt.Errorf("file close error: %s", err)
	}
	db.bufferRef = nil
	db.buffer = nil
	db.bufferSize = 0
	db.header = nil
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

func safeStringFromEncoded(key []byte) string {
	if len(key)&1 == 1 {
		key = append(key, 0)
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
	return string(ret)
}

func (db *DB) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return db.header.root.getNode(db.allocator).Get(db, k)
}

func (db *DB) CreateTable(table string) error {
	snap := db.GetRootSnapshot()
	err := snap.CreateTable(table)
	if err != nil {
		snap.Release()
		return err
	}
	db.SetRootSnapshot(snap)
	return nil
}

func (db *DB) CreateIndex(index IndexField) error {
	snap := db.GetRootSnapshot()
	err := snap.CreateIndex(index)
	if err != nil {
		snap.Release()
		return err
	}
	db.SetRootSnapshot(snap)
	return nil
}

func (db *DB) HasTable(table string) bool {
	snap := db.GetRootSnapshot()
	exists := snap.HasTable(table)
	snap.Release()
	return exists
}

func (db *DB) Iter() *Iterator {
	iter := db.header.root.getNode(db.allocator).Iterator(db.allocator)
	return iter
}

func (db *DB) Snapshot(id uint64) *Snapshot {
	if id == 0 {
		db.header.root.getNode(db.allocator).Retain()

		return &Snapshot{
			db:   db,
			root: db.header.root,
		}
	}

	ptr := Ptr(id)
	ptr.getNode(db.allocator).Retain()

	return &Snapshot{
		db:   db,
		root: ptr,
	}
}

func (db *DB) GetRootSnapshot() *Snapshot {
	db.header.root.getNode(db.allocator).Retain()

	return &Snapshot{
		db:   db,
		root: db.header.root,
	}
}

func (db *DB) SetRootSnapshot(s *Snapshot) {
	db.header.root.NodeRelease(db.allocator)
	db.header.root = *s.Root()
	db.header.root.getNode(db.allocator).Retain()
}
