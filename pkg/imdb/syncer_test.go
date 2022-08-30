package imdb

import (
	"github.com/go-pg/pg/v10"
	"testing"
	"time"
)

func TestSyncer_Run(t *testing.T) {
	db := pg.Connect(&pg.Options{
		Addr:     ":5432",
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	defer db.Close()

	_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS ratings
				(
					imdb_id varchar(50) not null unique primary key,
					rating numeric not null,
					voted  numeric not null
				);
			`)
	if err != nil {
		t.Error(err)
	}

	now := time.Now()

	syncer := NewSyncer(db)
	err = syncer.Run()

	if err != nil {
		t.Fatal(err)
	}

	since := time.Since(now)
	t.Logf("time spend to sync: %s", since)
}

func TestSyncerDownload(t *testing.T) {
	s := NewSyncer(nil)

	now := time.Now()
	b, err := s.download()
	since := time.Since(now)
	if err != nil {
		t.Error(err)
	}

	t.Logf("time spend to download %d bytes: %s", b.Len(), since)
}
