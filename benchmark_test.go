package ebakusdb_test

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/harkal/ebakusdb"
)

var openDBSize = flag.Int("open_db_size", 128*1024*1024, "Number of entries in db for opening")
var writeSync = flag.Bool("write_sync", false, "sync writing")
var cacheSize = flag.Int("cache_size", 0, "Capacity for block cache")
var valueSize = flag.Int("value_size", 100, "Size of each value")
var batchCount = flag.Int("batch_count", 1, "Batch count per write")

var writeBufferSize = flag.Int("write_buffer_size", 0, "Write buffer size")
var bloomBits = flag.Int("bloom_bits", 0, "Bits per key for bloom filter")
var openFiles = flag.Int("open_files", 0, "Max number of open files")

var compression = flag.String("compression", "default", "")
var compressionRatio = flag.Float64("compression_ratio", 0.5, "")

func initOptions() {

}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = ' ' + byte(r.Intn('~'-' '+1))
	}
	return b
}

func compressibleBytes(r *rand.Rand, ratio float64, n int) []byte {
	m := maxInt(int(float64(n)*ratio), 1)
	p := randomBytes(r, m)
	b := make([]byte, 0, n+n%m)
	for len(b) < n {
		b = append(b, p...)
	}
	return b[:n]
}

type randomValueGenerator struct {
	b []byte
	k int
}

func (g *randomValueGenerator) Value(i int) []byte {
	i = (i * g.k) % len(g.b)
	return g.b[i : i+g.k]
}

func makeRandomValueGenerator(r *rand.Rand, ratio float64, valueSize int) randomValueGenerator {
	b := compressibleBytes(r, ratio, valueSize)
	max := maxInt(valueSize, 1024*1024)
	for len(b) < max {
		b = append(b, compressibleBytes(r, ratio, valueSize)...)
	}
	return randomValueGenerator{b: b, k: valueSize}
}

type entryGenerator interface {
	Key(i int) []byte
	Value(i int) []byte
}

type pairedEntryGenerator struct {
	keyGenerator
	randomValueGenerator
}

func newRandomEntryGenerator(n int) entryGenerator {
	//r := rand.New(rand.NewSource(time.Now().Unix()))
	r := rand.New(rand.NewSource(0))
	return &pairedEntryGenerator{
		keyGenerator:         newRandomKeyGenerator(n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

func newFullRandomEntryGenerator(start, n int) entryGenerator {
	//r := rand.New(rand.NewSource(time.Now().Unix()))
	r := rand.New(rand.NewSource(0))
	return &pairedEntryGenerator{
		keyGenerator:         newFullRandomKeyGenerator(start, n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

func newSequentialEntryGenerator(n int) entryGenerator {
	//r := rand.New(rand.NewSource(time.Now().Unix()))
	r := rand.New(rand.NewSource(0))
	return &pairedEntryGenerator{
		keyGenerator:         newSequentialKeyGenerator(n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

type keyGenerator interface {
	Key(i int) []byte
}

type randomKeyGenerator struct {
	n int
	b bytes.Buffer
	f string
	r *rand.Rand
}

func (g *randomKeyGenerator) Key(i int) []byte {
	i = g.r.Intn(g.n)
	g.b.Reset()
	fmt.Fprintf(&g.b, g.f, i)
	return g.b.Bytes()
}

func newRandomKeyGenerator(n int) keyGenerator {
	return &randomKeyGenerator{n: n, f: "%016d", r: rand.New(rand.NewSource( /*time.Now().Unix()*/ 0))}
}

func newMissingKeyGenerator(n int) keyGenerator {
	return &randomKeyGenerator{n: n, f: "%016d.", r: rand.New(rand.NewSource( /*time.Now().Unix()*/ 0))}
}

type fullRandomKeyGenerator struct {
	keys []int
	b    bytes.Buffer
}

func newFullRandomKeyGenerator(start, n int) keyGenerator {
	keys := make([]int, n)
	for i := 0; i < n; i++ {
		keys[i] = start + i
	}
	r := rand.New(rand.NewSource( /*time.Now().Unix()*/ 0))
	for i := 0; i < n; i++ {
		j := r.Intn(n)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return &fullRandomKeyGenerator{keys: keys}
}

func (g *fullRandomKeyGenerator) Key(i int) []byte {
	i = i % len(g.keys)
	i = g.keys[i]
	g.b.Reset()
	fmt.Fprintf(&g.b, "%016d", i)
	return g.b.Bytes()
}

type sequentialKeyGenerator struct {
	bytes.Buffer
}

func (g *sequentialKeyGenerator) Key(i int) []byte {
	g.Reset()
	fmt.Fprintf(g, "%016d", i)
	return g.Bytes()
}

func newSequentialKeyGenerator(n int) keyGenerator {
	return &sequentialKeyGenerator{}
}

func maxInt(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func doRead(b *testing.B, db *ebakusdb.DB, g keyGenerator, allowNotFound bool) {
	for i := 0; i < b.N; i++ {
		//println("========================================Getting", string(g.Key(i)))
		_, found := db.Get(g.Key(i))
		if !allowNotFound && !found {
			b.Fatalf("db get error: Key not found\n")
		}
	}
}

func doWrite(N int, db *ebakusdb.Snapshot, batchCount int, g entryGenerator) {
	for i := 0; i < N; i += batchCount {
		for j := 0; j < batchCount; j++ {
			k := g.Key(i + j)
			v := g.Value(i + j)

			db.Insert(k, v)
		}
	}
}

func doDelete(N int, db *ebakusdb.Snapshot, k int, g keyGenerator) {

	for i := 0; i < N; i += k {
		for j := 0; j < k; j++ {
			db.Delete(g.Key(i + j))
		}
	}
}

func createDB() (*ebakusdb.DB, string) {
	f, err := ioutil.TempFile("/tmp", "ebakus-benchmark-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	fname := f.Name()
	db, err := ebakusdb.Open(fname, 0, nil)
	if err != nil {
		panic(fmt.Errorf("create db %q error: %s\n", fname, err))
	}
	return db, fname
}

func newDB(b *testing.B) string {
	db, dir := createDB()
	defer runtime.GC()
	defer func() {
		if db != nil {
			db.Close()
			os.RemoveAll(dir)
		}
	}()
	snap := db.GetRootSnapshot()
	doWrite(b.N, snap, 1, newFullRandomEntryGenerator(0, b.N))
	db.SetRootSnapshot(snap)
	snap.Release()
	db.Close()
	db = nil
	return dir
}

func openDB(dir string, b *testing.B) *ebakusdb.DB {
	db, err := ebakusdb.Open(dir, 0666, nil)
	if err != nil {
		b.Fatalf("open db %q error: %s\n", dir, err)
	}
	return db
}

func openFullDB(b *testing.B) (*ebakusdb.DB, func()) {
	defer runtime.GC()
	defer b.ResetTimer()
	dir := newDB(b)
	ok := false
	defer func() {
		if !ok {
			os.Remove(dir)
		}
	}()
	db := openDB(dir, b)
	ok = true
	return db, func() { db.Close(); os.Remove(dir) }
}

func openEmptyDB(b *testing.B) (*ebakusdb.DB, func()) {
	defer b.ResetTimer()
	db, dir := createDB()
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func BenchmarkOpen(b *testing.B) {
	defer func(N int) {
		b.N = N
	}(b.N)
	n := b.N
	b.N = *openDBSize / *valueSize
	dir := newDB(b)
	b.N = n
	defer os.Remove(dir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		openDB(dir, b).Close()
	}
}

func BenchmarkSeekRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	it := db.Iter()
	//defer it.Release()
	for i := 0; i < b.N; i++ {
		it.SeekPrefix(g.Key(i))
	}
}

func BenchmarkReadHot(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	k := maxInt((b.N+99)/100, 1)
	g := newRandomKeyGenerator(k)
	doRead(b, db, g, false)
}

func BenchmarkReadRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	doRead(b, db, g, false)
}

func BenchmarkReadMissing(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newMissingKeyGenerator(b.N)
	doRead(b, db, g, true)
}

func BenchmarkReadReverse(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	it := db.Iter()
	//defer it.Release()

	for {
		_, _, found := it.Next()
		if found == false {
			break
		}
	}
}

func BenchmarkReadSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	it := db.Iter()
	//defer it.Release()

	for {
		_, _, found := it.Next()
		if found == false {
			break
		}
	}
}

func BenchmarkWriteRandom(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	g := newFullRandomEntryGenerator(0, b.N)
	b.ResetTimer()
	snap := db.GetRootSnapshot()
	doWrite(b.N, snap, maxInt(*batchCount, 1), g)
	db.SetRootSnapshot(snap)
	snap.Release()
}

func BenchmarkWriteSequential(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	g := newSequentialEntryGenerator(b.N)
	b.ResetTimer()
	snap := db.GetRootSnapshot()
	doWrite(b.N, snap, maxInt(*batchCount, 1), g)
	db.SetRootSnapshot(snap)
	snap.Release()
}

func BenchmarkConcurrentWriteRandom(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	k := runtime.GOMAXPROCS(-1)
	if k > b.N {
		k = b.N
	}
	var gens []entryGenerator
	start, step := 0, b.N/k
	for i := 0; i < k; i++ {
		gens = append(gens, newFullRandomEntryGenerator(start, step))
		start += step
	}
	runtime.GC()
	b.ResetTimer()
	defer func(n int) {
		b.N = n
	}(b.N)
	b.N = step
	var wg sync.WaitGroup
	wg.Add(len(gens))
	snap := db.GetRootSnapshot()
	for _, g := range gens {
		go func(g entryGenerator) {
			defer wg.Done()
			doWrite(b.N, snap, maxInt(*batchCount, 1), g)
		}(g)
	}
	wg.Wait()
	db.SetRootSnapshot(snap)
	snap.Release()
}

func BenchmarkDeleteRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	snap := db.GetRootSnapshot()
	doDelete(b.N, snap, maxInt(*batchCount, 1), newRandomKeyGenerator(b.N))
	db.SetRootSnapshot(snap)
	snap.Release()
}

func BenchmarkDeleteSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	snap := db.GetRootSnapshot()
	doDelete(b.N, snap, maxInt(*batchCount, 1), newSequentialKeyGenerator(b.N))
	db.SetRootSnapshot(snap)
	snap.Release()
}

func TestMain(m *testing.M) {
	flag.Parse()
	initOptions()
	os.Exit(m.Run())
}
