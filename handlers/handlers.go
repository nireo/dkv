package handlers

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/nireo/dkv/config"
	"github.com/nireo/dkv/db"
)

// Server contains handlers
type Server struct {
	db     *db.DB
	router *httprouter.Router
	shards *config.Shards
}

// NewServer returns a new instance of server given a database
func NewServer(db *db.DB, s *config.Shards) *Server {
	return &Server{
		db:     db,
		router: httprouter.New(),
		shards: s,
	}
}

// Get handles getting a key from the server
func (s *Server) Get(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectRequest(w, r, shard, http.MethodGet)
		return
	}

	value, err := s.db.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("key %s not found", key), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(value)
}

// Set handles placing a key into the database
func (s *Server) Set(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading request body", http.StatusInternalServerError)
		return
	}

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectRequest(w, r, shard, http.MethodPut)
		return
	}

	if err := s.db.Set(key, value); err != nil {
		http.Error(w, "error settings key", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "key %s has been set, shardId=%d", key, shard)
}

// redirectRequest redirects a request to a given shard.
func (s *Server) redirectRequest(w http.ResponseWriter, r *http.Request, shard int, method string) {
	url := "http://" + s.shards.Addresses[shard] + r.RequestURI

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		http.Error(w, "something went wrong, "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "something went wrong, "+err.Error(), http.StatusInternalServerError)
		return
	}

	defer resp.Body.Close()
	io.Copy(w, resp.Body)
}

// Delete handles the deletion of a key-value pair from the database
func (s *Server) Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	shard := s.shards.GetShardIndex(key)
	if shard != s.shards.Index {
		s.redirectRequest(w, r, shard, http.MethodDelete)
	}

	if err := s.db.Delete(key); err != nil {
		http.Error(w, "could not delete key from database", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Listen sets up all the routes and stars listening on a given address
func (s *Server) Listen(address string) error {
	s.router.GET("/v1/:key", s.Get)
	s.router.PUT("/v1/:key", s.Set)
	s.router.DELETE("/v1/:key", s.Delete)

	if err := http.ListenAndServe(address, s.router); err != nil {
		log.Fatalf("error listening on port %s, err: %s", address, err)
	}

	return nil
}
