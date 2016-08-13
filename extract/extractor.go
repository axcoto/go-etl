package extract

import (
	"../etl"
	"../monitor"
	"../types"

	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
)

//Run fetches data and put into channel for processing
func Run(etlSession *etl.Session, db *sqlx.DB) {
	extractChannel := etlSession.ExtractChannel
	table := etlSession.Get("table")

	defer etlSession.Wg.Done()
	defer close(extractChannel)

	offset := 0
	limit := 16
	for {
		log.Printf("Fetch params: limit %d offset %d", limit, offset)
		query := fmt.Sprintf("%s LIMIT %d OFFSET %d", types.Query(table), limit, offset)

		rows, err := db.Queryx(query)
		if !rows.Next() {
			log.Printf("No more rows to do. Stop at offset %d", offset)
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		for {
			row := make(map[string]interface{})
			err = rows.MapScan(row)
			log.Printf("Row= %#v\n\n", row)

			if err != nil {
				log.Fatalln(err)
			}
			extractChannel <- row
			if !rows.Next() {
				break
			}
		}

		monitor.Report(table, offset)
		offset = offset + limit
	}

}
