package main

import (
	"log"

	"github.com/nireo/dkv/db"
	"github.com/nireo/dkv/handlers"
)

func main() {
	dbPath := "./dbfile"
	db, err := db.NewDatabase(dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", dbPath, err)
	}
	defer db.Close()

	server := handlers.NewServer(db)
	if err := server.Listen("localhost:8080"); err != nil {
		log.Fatalf("error when running server, err: %s", err)
	}
}
