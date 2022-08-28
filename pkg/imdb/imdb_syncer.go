package imdb

import (
	"fmt"
	"github.com/go-pg/pg/v10"
	"strings"
)

func UpdateRatings(db *pg.DB, ratingsTable string) error {
	reader, err := GetRatings()
	if err != nil {
		return err
	}

	// skip header row
	reader = reader[1:][:]

	var str string
	for _, row := range reader {
		for _, value := range row {
			str += value
			str += "\n"
		}
	}
	str = strings.ReplaceAll(str, "\t", ",")
	r := strings.NewReader(str)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	queryToCreateTempTable := fmt.Sprintf(`
		CREATE TEMPORARY TABLE temp(
		    imdb_id varchar(50) not null unique primary key,
			rating varchar(50) not null,
			voted varchar(50) not null)
		    ON COMMIT DROP
		    `)

	_, err = tx.Exec(queryToCreateTempTable)
	if err != nil {
		return handleInternal(tx, err)
	}

	queryToCopyDataToTemp := fmt.Sprintf("COPY temp FROM STDIN WITH CSV")

	_, err = tx.CopyFrom(r, queryToCopyDataToTemp)
	if err != nil {
		return handleInternal(tx, err)
	}

	query := fmt.Sprintf(`INSERT INTO %s as r SELECT * FROM temp 
			ON CONFLICT (imdb_id)
			DO UPDATE SET rating = EXCLUDED.rating, voted = EXCLUDED.voted
			WHERE r.rating != EXCLUDED.rating OR r.voted != EXCLUDED.voted`,
		ratingsTable)

	_, err = tx.Model().Exec(query)
	if err != nil {
		return handleInternal(tx, err)
	}

	err = tx.Commit()
	if err != nil {
		return handleInternal(tx, err)
	}
	return nil
}

func handleInternal(tx *pg.Tx, err error) error {
	errInt := tx.Rollback()
	if errInt != nil {
		return errInt
	}
	return err
}
