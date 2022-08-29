package imdb

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"github.com/go-pg/pg/v10"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	datasetUrl = "https://datasets.imdbws.com/title.ratings.tsv.gz"
)

type Syncer struct {
}

func (is *Syncer) UpdateRatings(db *pg.DB, ratingsTable string) error {
	reader, err := is.download()
	if err != nil {
		return err
	}

	// skip header row
	reader = reader[1:][:]

	var builder bytes.Buffer
	for _, row := range reader {
		for _, value := range row {
			builder.WriteString(value)
			builder.WriteString("\n")
		}
	}
	str := builder.String()
	str = strings.ReplaceAll(str, "\t", ",")
	r := strings.NewReader(str)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		CREATE TEMPORARY TABLE temp(
		    imdb_id varchar(50) not null unique primary key,
			rating numeric not null,
			voted numeric not null)
		    ON COMMIT DROP`)
	if err != nil {
		defer func(tx *pg.Tx) {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}(tx)
	}

	_, err = tx.CopyFrom(r, "COPY temp FROM STDIN WITH CSV")
	if err != nil {
		defer func(tx *pg.Tx) {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}(tx)
	}

	query := fmt.Sprintf(`INSERT INTO %s as r SELECT * FROM temp 
			ON CONFLICT (imdb_id)
			DO UPDATE SET rating = EXCLUDED.rating, voted = EXCLUDED.voted
			WHERE r.rating != EXCLUDED.rating OR r.voted != EXCLUDED.voted`,
		ratingsTable)

	_, err = tx.Model().Exec(query)
	if err != nil {
		defer func(tx *pg.Tx) {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}(tx)
	}

	err = tx.Commit()
	if err != nil {
		defer func(tx *pg.Tx) {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}(tx)
	}
	return nil
}

func (is *Syncer) download() ([][]string, error) {
	client := http.Client{Timeout: time.Duration(10) * time.Second}

	resp, err := client.Get(datasetUrl)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

	reader, err := gzip.NewReader(resp.Body)
	csvReader := csv.NewReader(reader)

	ratings, err := csvReader.ReadAll()
	return ratings, err
}
