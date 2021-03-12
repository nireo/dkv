package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/nireo/dkv/db"
	"github.com/nireo/dkv/handlers"
	"github.com/nireo/dkv/shards"
)

// define command line flags
var (
	dbPath     = flag.String("db", "", "path to the database")
	address    = flag.String("addr", "localhost:8080", "address where the server will be hosted")
	configFile = flag.String("conf", "conf.json", "shards file for shards")
	shardName  = flag.String("shards", "", "the shards used for the data")
)

func parse() {
	flag.Parse()
	if *dbPath == "" {
		log.Fatal("database field cannot be empty")
	}
	if *shardName == "" {
		log.Fatal("shards field cannot be empty")
	}
}

func main() {
	parse()
	conf, err := shards.ParseConfigFile("./conf.json")
	if err != nil {
		log.Fatalf("could not parse shards file, err: %s", err)
	}

	shardsList, err := conf.ParseConfigShards(*shardName)
	if err != nil {
		log.Fatalf("error parsing shards, err: %s", err)
	}

	db, err := db.NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", *dbPath, err)
	}
	defer db.Close()

	log.Printf("starting shards: %d at %s", shardsList.Amount, shardsList.Addresses[shardsList.Index])

	srv := handlers.NewServer(db, shardsList)

	http.HandleFunc("/get", srv.GetHTTP)
	http.HandleFunc("/set", srv.GetHTTP)
	http.HandleFunc("/del", srv.DeleteHTTP)
	http.HandleFunc("/purge", srv.DeleteNotBelonging)

	log.Fatal(http.ListenAndServe(*address, nil))
}
