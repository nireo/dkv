package db

import "github.com/syndtr/goleveldb/leveldb"

// Bucket is a collection of records in the database
type Bucket struct {
	id []byte
	db *DB
}

// Set places a key into the bucket
func (b *Bucket) Set(key []byte, data []byte) error {
	if b.db.ronly {
		return ErrReadOnly
	}

	if len(key) == 0 {
		return ErrKeyLength
	}

	prefixedKey := b.bucketPrefix(key)

	return b.db.db.Put(prefixedKey, data, nil)
}

// Get gets a key from the bucket
func (b *Bucket) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyLength
	}

	prefixedKey := b.bucketPrefix(key)
	val, err := b.db.db.Get(prefixedKey, nil)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	return val, nil
}

// Delete delets a key from the bucket
func (b *Bucket) Delete(key []byte) error {
	if b.db.ronly {
		return ErrReadOnly
	}

	if len(key) == 0 {
		return ErrKeyLength
	}

	prefixedKey := b.bucketPrefix(key)
	return b.db.db.Delete(prefixedKey, nil)
}

// bucketPrefix adds the bucket's prefix to the beginning of the key.
func (b *Bucket) bucketPrefix(key []byte) []byte {
	// 2 is the bucket id size
	buf := make([]byte, len(key)+len(b.id))
	copy(buf, b.id[:])
	copy(buf[len(b.id):], key)

	return buf
}
