package db

import (
	"io/ioutil"
	"os"
	"testing"
)

func createTempDb(t *testing.T, readOnly bool) *DB {
	t.Helper()

	dir, err := ioutil.TempDir(os.TempDir(), "dkvdb")
	if err != nil {
		t.Fatalf("error creating temp file, err: %s", err)
	}
	defer os.Remove(dir)

	db, err := NewDatabase(dir, readOnly)
	if err != nil {
		t.Fatalf("could not create db instance, err: %s", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestBucketGetSet(t *testing.T) {
	db := createTempDb(t, false)

	// create a new bucket
	testBucket := db.CreateBucket([]byte("test"))

	if err := testBucket.Set([]byte("testkey"), []byte("testvalue")); err != nil {
		t.Fatalf("error placing key into the bucket, err: %s", err)
	}

	// we shouldn't be able to find the key in the database normally
	if _, err := db.Get("testkey"); err == nil {
		t.Fatalf("found key from database without bucket")
	}

	// now test that we can find the key from the bucket
	val, err := testBucket.Get([]byte("testkey"))
	if err != nil {
		t.Fatalf("wasn't able to find key from the database, err: %s", err)
	}

	if string(val) != "testvalue" {
		t.Fatalf("the values weren't equal")
	}
}

func TestBucketDelete(t *testing.T) {
	db := createTempDb(t, false)

	testBucket := db.CreateBucket([]byte("test"))

	if err := testBucket.Set([]byte("testkey"), []byte("testvalue")); err != nil {
		t.Fatalf("error placing key into database")
	}

	// now test deleting the key from the database
	if err := testBucket.Delete([]byte("testkey")); err != nil {
		t.Fatalf("wasn't able to delete key from database, err: %s", err)
	}

	if _, err := testBucket.Get([]byte("testkey")); err == nil {
		t.Fatalf("was able to find key-value after deletion")
	}
}
