package monitor

import (
	"github.com/nanobox-io/golang-scribble"
	"log"
	"os"
)

type Monitor struct {
	path string
	db   *scribble.Driver
}

var m Monitor

type tableStatus struct {
	offset int
}

func NewMonitor() *Monitor {
	m = Monitor{
		path: "status.json",
	}

	var err error
	m.db, err = scribble.New("./status", nil)
	if err != nil {
		log.Println("Fail to create status")
		log.Fatal(err)
	}

	if _, err := os.Stat(m.path); os.IsNotExist(err) {
		log.Println("status.json isn't exist. Creating ...")
	}

	return &m
}

// Report updates working status on a given table
func Report(table string, offset int) {
	m.db.Write("table", table, tableStatus{
		offset: offset,
	})
}
