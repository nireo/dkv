package db_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nireo/dkv/db"
)

func TestGetSet(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "dkvdb")
	if err != nil {
		t.Fatalf("error creating temp file, err: %s", err)
	}
	defer os.Remove(dir)

	db, err := db.NewDatabase(dir)
	if err != nil {
		t.Fatalf("could not create new database, err: %s", err)
	}
	defer db.Close()

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

func TestDelete(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "dkvdb")
	if err != nil {
		t.Fatalf("error creating temp file, err: %s", err)
	}
	defer os.Remove(dir)

	db, err := db.NewDatabase(dir)
	if err != nil {
		t.Fatalf("could not create new database, err: %s", err)
	}
	defer db.Close()

	if err := db.Set("test1", []byte("value1")); err != nil {
		t.Fatalf("could not write key: %v", err)
	}

	if _, err := db.Get("test1"); err != nil {
		t.Fatalf("error getting key, err: %s", err)
	}

	if err := db.Delete("test1"); err != nil {
		t.Fatalf("error deleting key-value pair, err: %s", err)
	}

	if _, err := db.Get("test1"); err == nil {
		t.Fatalf("get was not deleted")
	}
}
