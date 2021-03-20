package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/nireo/dkv/db"
	"github.com/nireo/dkv/handlers"
	"github.com/nireo/dkv/replica"
	"github.com/nireo/dkv/shards"
)

// define command-line flags
var (
	dbPath      = flag.String("db", "", "path to the database")
	address     = flag.String("addr", "localhost:8080", "address where the server will be hosted")
	configFile  = flag.String("conf", "conf.json", "shards file for shards")
	shardName   = flag.String("shards", "", "the shards used for the data")
	ronly       = flag.Bool("ronly", false, "set the database into read-only mode")
	replication = flag.Bool("replica", false, "run as read-only replica server")
)

// parse command-line flags
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
	// read the shard config from the conf.json file
	conf, err := shards.ParseConfigFile("./conf.json")
	if err != nil {
		log.Fatalf("could not parse shards file, err: %s", err)
	}

	// parse the shards
	shardsList, err := conf.ParseConfigShards(*shardName)
	if err != nil {
		log.Fatalf("error parsing shards, err: %s", err)
	}

	// create a new db instance based on the command-line flags
	db, err := db.NewDatabase(*dbPath, *ronly)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", *dbPath, err)
	}
	defer db.Close()

	log.Printf("starting shards: %d at %s", shardsList.Amount, shardsList.Addresses[shardsList.Index])

	if *replication {
		master, ok := shardsList.Addresses[shardsList.Index]
		if !ok {
			log.Fatalf("could not find master address: %s", err)
		}
		go replica.Loop(db, master)
	}

	srv := handlers.NewServer(db, shardsList)

	http.HandleFunc("/get", srv.Get)
	http.HandleFunc("/set", srv.Set)
	http.HandleFunc("/del", srv.Delete)
	http.HandleFunc("/purge", srv.DeleteNotBelonging)
	http.HandleFunc("/del-rep", srv.DeleteReplicationKey)
	http.HandleFunc("/next", srv.GetNextReplicationKey)

	log.Fatal(http.ListenAndServe(*address, nil))
}
