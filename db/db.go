// The database package contains the local database implementation and some 'abstractions' for the badger key-val store.

package db

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	ErrReadOnly  = errors.New("cannot make changes to database, since it is in read-only mode.")
	ErrKeyLength = errors.New("the key length cannot be 0")
	ErrNotFound  = errors.New("the key was not found")
)

// DB represents the database
type DB struct {
	db    *leveldb.DB
	ronly bool // indicator if the database is in the read-only mode
}

// Close closes the database connection
func (d *DB) Close() error {
	return d.db.Close()
}

// NewDatabase returns a new instance of a database
func NewDatabase(path string, ronly bool) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &DB{db: ldb, ronly: ronly}, nil
}

// Get finds a key-value pair from the database
func (d *DB) Get(key string) ([]byte, error) {
	data, err := d.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	return data, err
}

// DeleteNotBelonging deletes all the key-value pairs in which the key matches the
// doesntBelong function.
func (d *DB) DeleteNotBelonging(doesntBelong func(string) bool) error {
	if d.ronly {
		return ErrReadOnly
	}

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
	if d.ronly {
		return ErrReadOnly
	}

	return d.db.Put([]byte(key), value, nil)
}

// CreateBucket creates a pointer to a bucket
func (d *DB) CreateBucket(identifier []byte) *Bucket {
	return &Bucket{
		db: d,
		id: identifier,
	}
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	if d.ronly {
		return ErrReadOnly
	}

	return d.db.Delete([]byte(key), nil)
}
