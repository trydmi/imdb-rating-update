package imdb

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"net/http"
)

const (
	datasetUrl = "https://datasets.imdbws.com/title.ratings.tsv.gz"
)

func GetRatings() ([][]string, error) {
	resp, err := http.Get(datasetUrl)
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
