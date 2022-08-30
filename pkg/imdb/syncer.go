package imdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"github.com/go-pg/pg/v10"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	datasetUrl = "https://datasets.imdbws.com/title.ratings.tsv.gz"
	timeout    = time.Second * 10
)

type Syncer struct {
	db *pg.DB
	hc *http.Client
}

// NewSyncer is a function that acts as a constructor for Syncer
func NewSyncer(db *pg.DB) *Syncer {
	return &Syncer{
		db: db,
		hc: &http.Client{
			Timeout: timeout,
		},
	}
}

// Run is a function that runs syncing process
func (is *Syncer) Run() error {
	buf, err := is.download()
	if err != nil {
		return err
	}
	return is.update(buf)
}

// update is an internal function that updates data in db
func (is *Syncer) update(r io.Reader) error {
	return is.db.RunInTransaction(context.Background(), func(tx *pg.Tx) error {
		// create temp table
		if _, err := tx.Exec(`
		CREATE TEMPORARY TABLE temp(
		    imdb_id varchar(50) not null unique primary key,
			rating numeric not null,
			voted numeric not null)
		    ON COMMIT DROP`); err != nil {
			return err
		}

		// copy from csv to temp table
		if _, err := tx.CopyFrom(r, "COPY temp FROM STDIN WITH CSV HEADER"); err != nil {
			return err
		}

		// insert new ratings
		query := `INSERT INTO ratings as r 
    				SELECT temp.* FROM temp 
    					LEFT JOIN ratings using (imdb_id)
					WHERE ratings.imdb_id is null
					`
		if _, err := tx.Exec(query); err != nil {
			return err
		}

		// update main table
		query = `
			UPDATE ratings
			SET rating = temp.rating, voted = temp.voted
			FROM temp 
			WHERE temp.imdb_id = ratings.imdb_id 
			AND ( temp.rating != ratings.rating 
			OR temp.voted != ratings.voted)   
			`
		if _, err := tx.Exec(query); err != nil {
			return err
		}
		return nil
	})
}

// download is a function that downloads imdb ratings data from imdb and returns ready-to-insert csv.
func (is *Syncer) download() (buf *bytes.Buffer, err error) {
	resp, err := is.hc.Get(datasetUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	reader, err := gzip.NewReader(resp.Body)
	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t'

	buf = &bytes.Buffer{}
	for {
		record, er := csvReader.Read()

		if er == io.EOF {
			break
		} else if er != nil {
			return buf, err
		}

		buf.WriteString(strings.Join(record, ","))
		buf.WriteByte('\n')
	}

	return
}
