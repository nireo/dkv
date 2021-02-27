package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

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
	confFile, err := ioutil.ReadFile("./conf.json")
	if err != nil {
		log.Fatalf("error reading configuration file, err: %s", err)
	}

	var conf config.Config
	if err := json.Unmarshal(confFile, &conf); err != nil {
		log.Fatalf("error parsing configuration data, err: %s", err)
	}

	index := -1
	var addresses = make(map[int]string)
	for _, s := range conf.Shards {
		addresses[s.Index] = s.Address
		if s.Name == *shard {
			index = s.Index
		}
	}

	if index == -1 {
		log.Fatalf("could not find shard %q in shard list", *shard)
	}

	dbPath := "./dbfile"
	db, err := db.NewDatabase(dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", dbPath, err)
	}
	defer db.Close()

	server := handlers.NewServer(db, index, len(conf.Shards), addresses)
	if err := server.Listen("localhost:8080"); err != nil {
		log.Fatalf("error when running server, err: %s", err)
	}
}
