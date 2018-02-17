package ebakusdb

type Snapshot struct {
	db   *DB
	root Ptr
}

func (snap *Snapshot) Release() {
	snap.root.NodeRelease(snap.db.allocator)
}

func (snap *Snapshot) GetId() uint64 {
	return snap.root.Offset
}

func (snap *Snapshot) Txn() *Txn {
	txn := &Txn{
		db:   snap.db,
		root: snap.root,
		snap: snap,
	}
	txn.root.getNode(snap.db.allocator).Retain()
	return txn
}

func (snap *Snapshot) Get(k []byte) (*[]byte, bool) {
	k = encodeKey(k)
	return snap.root.getNode(snap.db.allocator).Get(snap.db, k)
}

func (snap *Snapshot) CreateTable(table string) error {
	txn := snap.Txn()
	err := txn.CreateTable(table)
	if err != nil {
		txn.Rollback()
		return err
	}
	_, err = txn.Commit()
	return err
}

func (snap *Snapshot) CreateIndex(index IndexField) error {
	txn := snap.Txn()
	err := txn.CreateIndex(index)
	if err != nil {
		txn.Rollback()
		return err
	}
	_, err = txn.Commit()
	return err
}

func (snap *Snapshot) HasTable(table string) bool {
	txn := snap.Txn()
	exists := txn.HasTable(table)
	txn.Rollback()
	return exists
}

func (snap *Snapshot) Iter() *Iterator {
	iter := snap.root.getNode(snap.db.allocator).Iterator(snap.db.allocator)
	return iter
}

func (snap *Snapshot) Snapshot() *Snapshot {
	snap.root.getNode(snap.db.allocator).Retain()

	return &Snapshot{
		db:   snap.db,
		root: snap.root,
	}
}

func (snap *Snapshot) ResetTo(to *Snapshot) {
	snap.Release()
	snap.root = to.root
	snap.root.getNode(snap.db.allocator).Retain()
}
