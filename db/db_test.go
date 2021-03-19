package db_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nireo/dkv/db"
)

func createTestDatabase(t *testing.T, ronly bool) *db.DB {
	t.Helper()
	dir, err := ioutil.TempDir(os.TempDir(), "dkvdb")
	if err != nil {
		t.Fatalf("error creating temp file, err: %s", err)
	}
	t.Cleanup(func() { os.Remove(dir) })

	db, err := db.NewDatabase(dir, ronly)
	if err != nil {
		t.Fatalf("could not create new database, err: %s", err)
	}
	t.Cleanup(func() { db.Close() })

	return db
}

func TestGetSet(t *testing.T) {
	db := createTestDatabase(t, false)

	if err := db.Set("test1", []byte("value1")); err != nil {
		t.Fatalf("could not write key: %v", err)
	}

	value, err := db.Get("test1")
	if err != nil {
		t.Fatalf("error getting key, err: %s", err)
	}

	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("key values do not match.")
	}
}

func setKey(t *testing.T, d *db.DB, key, val string) {
	t.Helper()

	if err := d.Set(key, []byte(val)); err != nil {
		t.Fatalf("error setting key-val pair (%s, %s), err: %s", key, val, err)
	}
}

func TestDelete(t *testing.T) {
	db := createTestDatabase(t, false)

	setKey(t, db, "test1", "value1")
	if _, err := db.Get("test1"); err != nil {
		t.Fatalf("error while getting key, err: %s", err)
	}

	if err := db.Delete("test1"); err != nil {
		t.Fatalf("error deleting key-value pair, err: %s", err)
	}

	if _, err := db.Get("test1"); err == nil {
		t.Fatalf("get was not deleted")
	}
}

func TestDeleteAll(t *testing.T) {
	db := createTestDatabase(t, false)

	setKey(t, db, "testval1", "nothing1")
	setKey(t, db, "yessir", "nothing2")

	doesntBelong := (func(name string) bool {
		return name == "yessir"
	})

	if err := db.DeleteNotBelonging(doesntBelong); err != nil {
		t.Fatalf("could not delete non-belonging keys, err: %s", err)
	}

	if _, err := db.Get("testval1"); err != nil {
		t.Fatalf("could not find 'testval1', err: %s", err)
	}

	if _, err := db.Get("yessir"); err == nil {
		t.Fatalf("could find yessir even though it should be deleted, err: %s", err)
	}
}

func TestReadOnly(t *testing.T) {
	db := createTestDatabase(t, true)

	if err := db.Set("testkey", []byte("testval")); err == nil {
		t.Fatalf("was able to write to dabase in read-only mode")
	}

	if _, err := db.Get("testkey"); err == nil {
		t.Fatalf("was able to read new value in read-only mode")
	}

	doesntBelong := (func(name string) bool {
		return name == "testkey"
	})

	if err := db.DeleteNotBelonging(doesntBelong); err == nil {
		t.Fatalf("was able to delete values in read-only mode")
	}
}

func TestDeleteReplicationKey(t *testing.T) {
	db := createTestDatabase(t, false)
	setKey(t, db, "testkey", "testval")

	k, v, err := db.GetNextReplica()
	if err != nil {
		t.Fatalf("cannot get next key from replication: %s", err)
	}

	if !bytes.Equal(k, []byte("testkey")) || !bytes.Equal(v, []byte("testval")) {
		t.Errorf("wrong values from get next function. got=%q-%q; want=%q-%q", k, v, "party", "Great")
	}

	if err := db.DeleteReplicationKey([]byte("testkey"), []byte("valtest")); err == nil {
		t.Fatalf("deleteing a non-existant replication key didn't return an error.")
	}

	if err := db.DeleteReplicationKey([]byte("testkey"), []byte("testval")); err != nil {
		t.Fatalf("deleteing a non-existant replication key didn't return an error.")
	}

	key, val, err := db.GetNextReplica()
	if err != nil {
		t.Fatalf("cannot get next key from replication: %s", err)
	}

	if key != nil || val != nil {
		t.Fatalf("next replication values are not nil")
	}
}
