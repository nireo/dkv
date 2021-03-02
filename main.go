package main

import (
	"flag"
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
	conf, err := config.ParseConfigFile("./conf.json")
	if err != nil {
		log.Fatalf("could not parse config file, err: %s", err)
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

	db, err := db.NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", *dbPath, err)
	}
	defer db.Close()

	log.Printf("starting shard: %s at %s", conf.Shards[index].Name, conf.Shards[index].Address)

	server := handlers.NewServer(db, index, len(conf.Shards), addresses)
	if err := server.Listen(*address); err != nil {
		log.Fatalf("error when running server, err: %s", err)
	}
}
