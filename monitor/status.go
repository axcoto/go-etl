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
		path: "./status",
	}

	var err error
	m.db, err = scribble.New(m.path, nil)
	if err != nil {
		log.Println("Fail to create status db")
		log.Fatal(err)
	}

	if _, err := os.Stat(m.path); os.IsNotExist(err) {
		log.Printf("Creating status db...")
	}

	return &m
}

// Report updates working status on a given table
func Report(table string, offset int) {
	s := tableStatus{
		offset: offset,
	}
	if err := m.db.Write("table", table, s); err != nil {
		log.Fatal("Fail to write %v", err)
	}

}
