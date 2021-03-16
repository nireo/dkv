// The database package contains the local database implementation and some 'abstractions' for the badger key-val store.

package db

import (
	"bytes"
	"errors"
	"log"
	"math"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// errors
	ErrReadOnly     = errors.New("cannot make changes to database, since it is in read-only mode.")
	ErrKeyLength    = errors.New("the key length cannot be 0")
	ErrNotFound     = errors.New("the key was not found")
	ErrBucketName   = errors.New("the bucket name is too short")
	ErrNoFirstKey   = errors.New("there are no keys in the bucket")
	ErrValDontMatch = errors.New("values don't match")

	replicaBucket = "re"
	defaultBucket = "de"
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

// GetLevelDB returns a pointer to the underlying levelDB database
// this is mostly used for testing if buckets really insert into buckets
func (d *DB) GetLevelDB() *leveldb.DB {
	return d.db
}

// NewDatabase returns a new instance of a database
func NewDatabase(path string, ronly bool) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	d := &DB{db: ldb, ronly: ronly}

	d.buckets = make(map[string][]byte)

	// create the default bucket
	if _, err := d.newBucket(defaultBucket); err != nil {
		return nil, err
	}

	// create the replication bucket
	if _, err := d.newBucket("re"); err != nil {
		return nil, err
	}

	return d, nil
}

// Get finds a key-value pair from the database
func (d *DB) Get(key string) ([]byte, error) {
	data, err := d.Bucket(defaultBucket).Get([]byte(key))
	if err != nil {
		return nil, err
	}

	return data, nil
}

// DeleteNotBelonging deletes all the key-value pairs in which the key matches the
// doesntBelong function.
func (d *DB) DeleteNotBelonging(doesntBelong func(string) bool) error {
	if d.ronly {
		return ErrReadOnly
	}

	iter := d.db.NewIterator(util.BytesPrefix([]byte(defaultBucket)), nil)
	var keys []string
	for iter.Next() {
		// remove the bucket id from the key name
		key := string(iter.Key())[2:]
		if doesntBelong(key) {
			keys = append(keys, key)
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	for _, key := range keys {
		// delete the key straight using the database since the bucket implementation
		// is used for all the operations, meaning that the bucket prefix would just
		// be appended to the start two times.
		log.Println("deleting", key)
		if err := d.Bucket(defaultBucket).Delete([]byte(key)); err != nil {
			return err
		}
	}

	return nil
}

// Bucket is the common method for doing operations on a bucket for example:
// d.Bucket(defaultBucket).Get(key)
func (d *DB) Bucket(name string) *Bucket {
	bucket, ok := d.bucket(name)
	if !ok {
		bucket, _ := d.newBucket(name)
		return bucket
	}
	return bucket
}

// bucket finds a bucket with a given name and returns a ok flag depending if
// a bucket was found
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

// newBucket creates a new bucket and returns an error if any
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

	if err := d.Bucket(defaultBucket).Set([]byte(key), value); err != nil {
		return err
	}

	return d.Bucket(replicaBucket).Set([]byte(key), value)
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	if d.ronly {
		return ErrReadOnly
	}

	return d.Bucket(defaultBucket).Delete([]byte(key))
}

// GetNext returns the key-value pair that has changed and has not yet applied to replicas.
func (d *DB) GetNextReplica() (key, value []byte, err error) {
	iter := d.db.NewIterator(util.BytesPrefix([]byte(replicaBucket)), nil)
	if ok := iter.First(); !ok {
		return nil, nil, ErrNoFirstKey
	}

	k := iter.Key()
	v := iter.Value()
	key = copyBytes(k)
	v = copyBytes(v)

	iter.Release()

	return key, value, nil
}

// DeleteReplication deletes the key from the replication queue.
func (d *DB) DeleteReplicationKey(key, val []byte) (err error) {
	value, err := d.Bucket(replicaBucket).Get(key)
	if err != nil {
		return err
	}

	if !bytes.Equal(value, val) {
		return ErrValDontMatch
	}

	return d.Bucket(replicaBucket).Delete(key)
}

// SetOnReplica sets the key to the requested value into the default database
func (d *DB) SetOnReplica(key string, val []byte) error {
	return d.Bucket(defaultBucket).Set([]byte(key), val)
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)

	return res
}
