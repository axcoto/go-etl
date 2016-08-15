package monitor

import (
	"github.com/nanobox-io/golang-scribble"
	"log"
	"os"
	"time"
)

type Monitor struct {
	path string
	db   *scribble.Driver
}

var (
	m     Monitor
	start time.Time
)

type tableStatus struct {
	Offset int
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
		Offset: offset,
	}
	if err := m.db.Write("table", table, s); err != nil {
		log.Fatal("Fail to write %v", err)
	}
}

// GetTableProgress return position where we left off
func GetTableProgress(table string) int {
	s := tableStatus{}
	if err := m.db.Read("table", table, &s); err != nil {
		log.Println("No existing progress found: %s. Will start from offset 0", err)
		return 0
	}

	return s.Offset
}

func Start() {
	start = time.Now()
}

func Done() {
	log.Printf("ETL takes %s", time.Since(start))
}
