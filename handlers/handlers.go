package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/nireo/dkv/db"
	"github.com/nireo/dkv/replica"
	"github.com/nireo/dkv/shards"
)

// Server contains handlers
type Server struct {
	db     *db.DB
	shards *shards.Shards
}

// NewServer returns a new instance of server given a database
func NewServer(db *db.DB, s *shards.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

// Get takes a key as a url parameter and return the value in the request body
func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectHTTP(shard, w, r)
		return
	}

	value, err := s.db.Get(key)
	if err != nil {
		http.Error(w, "error finding key from database"+err.Error(), http.StatusNotFound)
		return
	}

	w.Write(value)
}

// Set takes in a key-value pair as url parameters and creates a key-value pair
// into the database.
func (s *Server) Set(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectHTTP(shard, w, r)
		return
	}

	if err := s.db.Set(key, []byte(value)); err != nil {
		http.Error(w, "error setting value, err: %s"+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("shards sent to" + strconv.Itoa(shard)))
}

func (s *Server) redirectHTTP(shard int, w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "redirecting from shards %d to shards %d", s.shards.Index, shard)
	resp, err := http.Get("http://" + s.shards.Addresses[shard] + r.RequestURI)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	io.Copy(w, resp.Body)
}

// Delete takes in a key as an url parameter and removes that key from the database
func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectHTTP(shard, w, r)
		return
	}

	if err := s.db.Delete(key); err != nil {
		http.Error(w, "could not delete key"+err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetNextReplicationKey returns the next key in the replication queue
func (s *Server) GetNextReplicationKey(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	k, v, err := s.db.GetNextReplica()
	if err != nil {
		http.Error(w, "could not retrieve next replication key: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	enc.Encode(&replica.Next{
		Key:   string(k),
		Value: string(v),
	})
}

// DeleteReplicationKey removes given key-value pair from the replication queue
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	if err := s.db.DeleteReplicationKey([]byte(key), []byte(value)); err != nil {
		http.Error(w, "could not delete replication key: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DeleteNotBelonging removes all of the values in the database that don't match with the
// shard hash.
func (s *Server) DeleteNotBelonging(w http.ResponseWriter, r *http.Request) {
	doesntBelong := (func(key string) bool {
		return s.shards.GetShardIndex(key) != s.shards.Index
	})

	if err := s.db.DeleteNotBelonging(doesntBelong); err != nil {
		http.Error(w, "error while deleting non-belonging keys, err: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
