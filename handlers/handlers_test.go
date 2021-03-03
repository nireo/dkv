package handlers

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/nireo/dkv/config"
	"github.com/nireo/dkv/db"
)

func createShardDb(t *testing.T, id int) *db.DB {
	t.Helper()

	dir, err := ioutil.TempDir(os.TempDir(), fmt.Sprintf("db%d", id))
	if err != nil {
		t.Fatalf("could not create a temp directory")
	}

	t.Cleanup(func() {
		os.Remove(dir)
	})

	db, err := db.NewDatabase(dir)
	if err != nil {
		t.Fatalf("could not create new database, err: %s", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func createTestServer(t *testing.T, id int, addresses map[int]string) (*db.DB, *Server) {
	t.Helper()

	db := createShardDb(t, id)
	cfg := &config.Shards{
		Addresses: addresses,
		Amount:    len(addresses),
		Index:     id,
	}

	s := NewServer(db, cfg)
	return db, s
}

func TestServerGetSet(t *testing.T) {
	var test1GetHandler, test1SetHandler func(w http.ResponseWriter, r *http.Request)
	var test2GetHandler, test2SetHandler func(w http.ResponseWriter, r *http.Request)

	test1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			test1GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			test1SetHandler(w, r)
		}
	}))
	defer test1.Close()

	test2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			test2GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			test2SetHandler(w, r)
		}
	}))
	defer test2.Close()

	addresses := map[int]string{
		0: strings.TrimPrefix(test1.URL, "http://"),
		1: strings.TrimPrefix(test2.URL, "http://"),
	}

	// testvalue1 goes to shard 1 and testvalue goes to shard 0 due to the hashing function
	db1, sr1 := createTestServer(t, 0, addresses)
	db2, sr2 := createTestServer(t, 1, addresses)

	test1GetHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := []httprouter.Param{
			{Key: "key", Value: "testvalue1"},
		}
		sr1.Get(w, r, params)
	})

	test1SetHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := []httprouter.Param{
			{Key: "key", Value: "testvalue1"},
		}
		r.Body = ioutil.NopCloser(strings.NewReader("testvalue1"))
		sr1.Set(w, r, params)
	})

	test2GetHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := []httprouter.Param{
			{Key: "key", Value: "testvalue"},
		}
		sr2.Get(w, r, params)
	})

	test2SetHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := []httprouter.Param{
			{Key: "key", Value: "testvalue"},
		}
		r.Body = ioutil.NopCloser(strings.NewReader("testvalue"))
		sr2.Set(w, r, params)
	})

	if _, err := http.Post(test1.URL+"/set", "text/plain", nil); err != nil {
		t.Fatalf("could not send set request, err: %s", err)
	}

	if _, err := http.Post(test2.URL+"/set", "text/plain", nil); err != nil {
		t.Fatalf("could not send set request, err: %s", err)
	}

	if _, err := db1.Get("testvalue"); err != nil {
		t.Fatalf("could not find 'testvalue' from db1, err: %s", err)
	}

	if _, err := db2.Get("testvalue1"); err != nil {
		t.Fatalf("could not find 'testvalue1' from db2, err: %s", err)
	}
}
