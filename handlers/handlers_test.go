package handlers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/nireo/dkv/shards"
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
	cfg := &shards.Shards{
		Addresses: addresses,
		Amount:    len(addresses),
		Index:     id,
	}

	s := NewServer(db, cfg)
	return db, s
}

func TestServer(t *testing.T) {
	var ts1GetHandler, ts1SetHandler func(w http.ResponseWriter, r *http.Request)
	var ts2GetHandler, ts2SetHandler func(w http.ResponseWriter, r *http.Request)

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			ts1GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			ts1SetHandler(w, r)
		}
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			ts2GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			ts2SetHandler(w, r)
		}
	}))
	defer ts2.Close()

	addrs := map[int]string{
		0: strings.TrimPrefix(ts1.URL, "http://"),
		1: strings.TrimPrefix(ts2.URL, "http://"),
	}

	db1, web1 := createTestServer(t, 0, addrs)
	db2, web2 := createTestServer(t, 1, addrs)

	keys := map[string]int{
		"testvalue1": 1,
		"testvalue":  0,
	}

	ts1GetHandler = web1.GetHTTP
	ts1SetHandler = web1.SetHTTP
	ts2GetHandler = web2.GetHTTP
	ts2SetHandler = web2.SetHTTP

	for key := range keys {
		_, err := http.Get(fmt.Sprintf(ts1.URL+"/set?key=%s&value=value-%s", key, key))
		if err != nil {
			t.Fatalf("Could not set the key %q: %v", key, err)
		}
	}

	for key := range keys {
		resp, err := http.Get(fmt.Sprintf(ts1.URL+"/get?key=%s", key))
		if err != nil {
			t.Fatalf("Get key %q error: %v", key, err)
		}
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Could read contents of the key %q: %v", key, err)
		}

		want := []byte("value-" + key)
		if !bytes.Contains(contents, want) {
			t.Errorf("Unexpected contents of the key %q: got %q, want the result to contain %q", key, contents, want)
		}

		log.Printf("Contents of key %q: %s", key, contents)
	}

	value1, err := db1.Get("testvalue")
	if err != nil {
		t.Fatalf("USA key error: %v", err)
	}

	want1 := "value-testvalue"
	if !bytes.Equal(value1, []byte(want1)) {
		t.Errorf("Unexpected value of USA key: got %q, want %q", value1, want1)
	}

	value2, err := db2.Get("testvalue1")
	if err != nil {
		t.Fatalf("Soviet key error: %v", err)
	}

	want2 := "value-testvalue1"
	if !bytes.Equal(value2, []byte(want2)) {
		t.Errorf("Unexpected value of Soviet key: got %q, want %q", value2, want2)
	}
}
