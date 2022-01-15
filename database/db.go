package database

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DataBase interface {
	Put(name string, value []byte) error
	Get(name string) ([]byte, error)
	Delete(name string) error
	NewIterator(slice *util.Range) iterator.Iterator
	Close() error
}

func Open(path string) DataBase {
	leveldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic("open db failed")
	}
	db := &database{
		db:     leveldb,
		path:   path,
		closed: false,
	}
	return db
}

type database struct {
	db     *leveldb.DB
	path   string
	closed bool
	mu     *sync.Mutex
}

func (d *database) Close() error {
	err := d.db.Close()
	if err != nil {
		return err
	}
	d.closed = true
	return nil
}

func (d *database) Put(name string, value []byte) error {
	err := d.db.Put([]byte(name), value, nil)
	if err != nil {
		return err
	}
	return nil
}

func (d *database) Get(name string) ([]byte, error) {
	value, err := d.db.Get([]byte(name), nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (d *database) Delete(name string) error {
	err := d.db.Delete([]byte(name), nil)
	if err != nil {
		return err
	}
	return nil
}

func (d *database) NewIterator(slice *util.Range) iterator.Iterator {
	return d.db.NewIterator(slice, nil)
}
