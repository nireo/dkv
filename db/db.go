// The database package contains the local database implementation and some 'abstractions' for the badger key-val store.

package db

import (
	"github.com/syndtr/goleveldb/leveldb"
)

// DB represents the database
type DB struct {
	db *leveldb.DB
}

// Close closes the database connection
func (d *DB) Close() error {
	return d.db.Close()
}

// NewDatabase returns a new instance of a database
func NewDatabase(path string) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &DB{db: ldb}, nil
}

// Get finds a key-value pair from the database
func (d *DB) Get(key string) ([]byte, error) {
	data, err := d.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	return data, err
}

func (d *DB) DeleteNotBelonging(doesntBelong func(string) bool) error {
	iter := d.db.NewIterator(nil, nil)
	var keys []string
	for iter.Next() {
		key := string(iter.Key())
		if doesntBelong(key) {
			keys = append(keys, key)
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	for _, key := range keys {
		if err := d.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// Set creates a key-value entry in the database
func (d *DB) Set(key string, value []byte) error {
	return d.db.Put([]byte(key), value, nil)
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	return d.db.Delete([]byte(key), nil)
}
