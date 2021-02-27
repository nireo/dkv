package main

import (
	"log"

	"github.com/nireo/dkv/db"
)

func main() {
	dbPath := "./dbfile"
	db, err := db.NewDatabase(dbPath)
	if err != nil {
		log.Fatalf("error opening db: %s, err: %s", dbPath, err)
	}
	defer db.Close()
}
