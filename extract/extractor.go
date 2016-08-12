package extract

import (
	"../monitor"
	"../types"

	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
)

//PrePop fetches data and put into channel for processing
func PrePop(table string, extractChannel chan map[string]interface{}, db *sqlx.DB) {
	offset := 0
	limit := 16
	for {
		log.Printf("Fetch params: limit %d offset %d", limit, offset)
		query := fmt.Sprintf("%s LIMIT %d OFFSET %d", types.PrePopSql, limit, offset)

		rows, err := db.Queryx(query)
		if !rows.Next() {
			log.Printf("No more rows to do. Stop at offset %d", offset)
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		for {
			appPrePop := types.AppPrepop{}

			row := make(map[string]interface{})
			err = rows.MapScan(row)
			log.Printf("Row= %#v\n\n", row)

			err = rows.StructScan(&appPrePop)
			if err != nil {
				log.Fatalln(err)
			}
			log.Println("Write ID: " + appPrePop.GitId.String)
			extractChannel <- row
			log.Println("Fetch next row")
			if !rows.Next() {
				break
			}
		}

		monitor.Report("prepop", offset)
		offset = offset + limit
	}
	//close(extractChannel)
}
