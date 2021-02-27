package handlers

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/nireo/dkv/db"
)

// Server contains handlers
type Server struct {
	db     *db.DB
	router *httprouter.Router
}

// NewServer returns a new instance of server given a database
func NewServer(db *db.DB) *Server {
	return &Server{
		db:     db,
		router: httprouter.New(),
	}
}

// Get handles getting a key from the server
func (s *Server) Get(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
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

	if err := s.db.Set(key, value); err != nil {
		http.Error(w, "error settings key", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "key %s has been set", key)
}

// Delete handles the deletion of a key-value pair from the database
func (s *Server) Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
	if err := s.db.Delete(key); err != nil {
		http.Error(w, "could not delete key from database", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Listen(address string) error {
	s.router.GET("/v1/:key", s.Get)
	s.router.PUT("/v1/:key", s.Set)
	s.router.DELETE("/v1/:key", s.Delete)

	if err := http.ListenAndServe("127.0.0.1:8080", s.router); err != nil {
		log.Fatalf("error listening on port 8080, err: %s", err)
	}

	return nil
}
