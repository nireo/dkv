package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/nireo/dkv/config"
	"github.com/nireo/dkv/db"
	"github.com/nireo/dkv/handlers"
)

// define command line flags
var (
	dbPath     = flag.String("db", "", "path to the database")
	address    = flag.String("addr", "localhost:8080", "address where the server will be hosted")
	configFile = flag.String("conf", "conf.json", "config file for shards")
	shard      = flag.String("shard", "", "the shard used for the data")
)

func parse() {
	flag.Parse()
	if *dbPath == "" {
		log.Fatal("database field cannot be empty")
	}
	if *shard == "" {
		log.Fatal("shard field cannot be empty")
	}
}

func main() {
	parse()
	conf, err := config.ParseConfigFile("./conf.json")
	if err != nil {
		log.Fatalf("could not parse config file, err: %s", err)
	}

	shards, err := conf.ParseConfigShards(*shard)
	if err != nil {
		log.Fatalf("error parsing shard, err: %s", err)
	}

	db, err := db.NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", *dbPath, err)
	}
	defer db.Close()

	log.Printf("starting shard: %d at %s", shards.Amount, shards.Addresses[shards.Index])

	srv := handlers.NewServer(db, shards)

	http.HandleFunc("/get", srv.GetHTTP)
	http.HandleFunc("/set", srv.GetHTTP)
	http.HandleFunc("/del", srv.DeleteHTTP)

	log.Fatal(http.ListenAndServe(*address, nil))
}
