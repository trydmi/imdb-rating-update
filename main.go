package main

import (
	"github.com/go-pg/pg/v10"
	"imdb-rating-update/pkg/imdb"
	"log"
	"time"
)

func main() {
	db := pg.Connect(&pg.Options{
		Addr:     ":5432",
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	defer func(db *pg.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	now := time.Now()

	syncer := imdb.Syncer{}

	err := syncer.UpdateRatings(db, "ratings")
	if err != nil {
		log.Fatal(err)
	}

	since := time.Since(now)
	log.Print(since)
}
