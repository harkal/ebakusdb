package ebakusdb

import (
	"os"
	"time"

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
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	if options == nil {
		options = DefaultOptions
	}

	db := &DB{
		readOnly: options.ReadOnly,
	}

	buffer := make([]byte, 1024*1024)

	allocator, err := balloc.NewBufferAllocator(buffer)
	if err != nil {
		return nil, err
	}

	db.allocator = allocator

	return db, nil
}
