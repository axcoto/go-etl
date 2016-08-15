package main

import (
	"./extract"
	"./loader"
	"./monitor"
	"./transform"
	"os"
	"os/signal"
	//"strconv"
	"log"

	"./etl"
	"./types"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"flag"
)

var (
	config map[string]string

	queryScope string
	table      string
	startTs    string
	endTs      string
	version    string
)

func initConfig() {
	config = make(map[string]string)

	config["AWS_REGION"] = os.Getenv("AWS_REGION")

	config["PG_URI"] = os.Getenv("PG_URI")
	config["PG_FETCH_LIMIT"] = os.Getenv("PG_FETCH_LIMIT")
	if config["PG_FETCH_LIMIT"] == "" {
		config["PG_FETCH_LIMIT"] = "16"
	}

	config["DYNAMODB_HOST"] = os.Getenv("DYNAMODB_HOST")

	// Don't suport batch write atm
	config["DYNAMODB_BATCH_WRITE"] = "1" //os.Getenv("DYNAMODB_BATCH_WRITE")
	config["DYNAMODB_PARALLEL_FLUSH"] = os.Getenv("DYNAMODB_PARALLEL_FLUSH")
}

func init() {
	flag.StringVar(&table, "table", "", "Table on dynamodb to migrate data")
	flag.StringVar(&queryScope, "scope", "", "Scope the query")
	flag.StringVar(&startTs, "start", "", "Start time stamp in PST")
	flag.StringVar(&endTs, "end", "", "End timestamp in PST")
	flag.Parse()

	initConfig()
	monitor.NewMonitor()

	if config["PG_URI"] == "" {
		log.Fatal("Make sure you set environment var for postgress connection string")
	}
	if config["DYNAMODB_HOST"] == "" {
		log.Fatal("Make sure you set environment var for dynamodb host")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Printf("Catch signal: %s", sig)
			monitor.Done()
			log.Fatal("Force quit.")
		}
	}()
}

func main() {
	log.Println("ETL, revision ", version)

	if table == "" || startTs == "" || endTs == "" {
		log.Fatal("Not enough param. Please set table, start, end.Example:\n-table gloca_dev_web_application_prepop -start \"1970-1-1 0:0:0\" -end \"2020-1-1 0:0:0\"\n\n")
	}

	log.Printf("Query Filter:\n-->Start: %s\n-->End: %s\n=======================================\n", startTs, endTs)
	monitor.Start()

	db, err := sqlx.Open("postgres", config["PG_URI"])
	if err != nil {
		log.Println("Fail to connect to pg")
		log.Fatal(err)
	}
	db = db.Unsafe()

	sess, err := session.NewSession()
	if err != nil {
		log.Println("Fail to create aws session")
		log.Fatal(err)
	}

	svc := dynamodb.New(sess, &aws.Config{Region: aws.String(config["AWS_REGION"]), Endpoint: aws.String(config["DYNAMODB_HOST"])})

	if table == "all" {
		for _, table := range types.Table() {
			log.Printf("Init for table: %s\n", table)
			etlSession := etl.NewSession(config, table, queryScope)

			etlSession.SetParam("[START_TIMESTAMP_IN_PACIFIC_TIME]", startTs)
			etlSession.SetParam("[END_TIMESTAMP_IN_PACIFIC_TIME]", endTs)

			go extract.Run(etlSession, db)
			go transform.Run(etlSession)
			go loader.Run(etlSession, svc)
			etlSession.Wait()
		}
	} else {
		log.Printf("Init for table: %s\n", table)
		etlSession := etl.NewSession(config, table, queryScope)
		etlSession.SetParam("[START_TIMESTAMP_IN_PACIFIC_TIME]", startTs)
		etlSession.SetParam("[END_TIMESTAMP_IN_PACIFIC_TIME]", endTs)

		go extract.Run(etlSession, db)
		go transform.Run(etlSession)
		go loader.Run(etlSession, svc)
		etlSession.Wait()
	}

	monitor.Done()
}
