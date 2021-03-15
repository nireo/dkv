// The database package contains the local database implementation and some 'abstractions' for the badger key-val store.

package db

import (
	"errors"
	"math"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	// errors
	ErrReadOnly   = errors.New("cannot make changes to database, since it is in read-only mode.")
	ErrKeyLength  = errors.New("the key length cannot be 0")
	ErrNotFound   = errors.New("the key was not found")
	ErrBucketName = errors.New("the bucket name is too short")
)

// MaxBuckets is the maximum amount of buckets
const MaxBuckets = math.MaxUint16 - (8 * 256)

// DB represents the database
type DB struct {
	db    *leveldb.DB
	ronly bool // indicator if the database is in the read-only mode

	// buckets map maps to the identifiers, such that we can easily create a new bucket instance
	// it is currently a map since maybe in the future I will implement a better indexing solution
	buckets map[string][]byte
	bmutex  sync.RWMutex
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

func (d *DB) Bucket(name string) *Bucket {
	bucket, ok := d.bucket(name)
	if !ok {
		bucket, _ := d.newBucket(name)
		return bucket
	}
	return bucket
}

func (d *DB) bucket(name string) (*Bucket, bool) {
	d.bmutex.RLock()
	bucketId, ok := d.buckets[name]
	d.bmutex.RUnlock()
	if !ok {
		return nil, false
	}

	bucket := &Bucket{
		db: d,
		id: bucketId,
	}
	return bucket, true
}

func (d *DB) newBucket(name string) (*Bucket, error) {
	if len(name) == 0 {
		return nil, ErrBucketName
	}

	// check if a bucket already exists
	d.bmutex.RLock()
	if _, ok := d.buckets[name]; ok {
		return &Bucket{db: d, id: []byte(name)}, nil
	}
	d.bmutex.RUnlock()

	// write the bucket to the list of buckets
	d.bmutex.Lock()
	d.buckets[name] = []byte(name)
	d.bmutex.Unlock()

	return &Bucket{db: d, id: []byte(name)}, nil
}

// Set creates a key-value entry in the database
func (d *DB) Set(key string, value []byte) error {
	if d.ronly {
		return ErrReadOnly
	}

	return d.db.Put([]byte(key), value, nil)
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	if d.ronly {
		return ErrReadOnly
	}

	return d.db.Delete([]byte(key), nil)
}

func prefixKey(prefix, key []byte) []byte {
	buf := make([]byte, len(key)+len(prefix))
	copy(buf, prefix[:])
	copy(buf[len(prefix):], key)

	return buf
}
