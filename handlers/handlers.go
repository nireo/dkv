package handlers

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

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

func (s *Server) GetHTTP(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) SetHTTP(w http.ResponseWriter, r *http.Request) {
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
	w.Write([]byte("shard sent to" + strconv.Itoa(shard)))
}

func (s *Server) redirectHTTP(shard int, w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "redirecting from shard %d to shard %d", s.shards.Index, shard)
	resp, err := http.Get("http://" + s.shards.Addresses[shard] + r.RequestURI)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	io.Copy(w, resp.Body)
}

func (s *Server) DeleteHTTP(w http.ResponseWriter, r *http.Request) {
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
