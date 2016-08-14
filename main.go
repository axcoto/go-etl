package main

import (
	"./extract"
	"./loader"
	"./monitor"
	"./transform"
	"os"
	//"strconv"
	"log"
	"time"

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
	config     map[string]string
	queryScope string
)

func initConfig() {
	config = make(map[string]string)

	config["PG_URI"] = os.Getenv("PG_URI")
	config["PG_FETCH_LIMIT"] = os.Getenv("PG_FETCH_LIMIT")
	config["DYNAMODB_BATCH_WRITE"] = os.Getenv("DYNAMODB_BATCH_WRITE")
}

func init() {
	flag.StringVar(&queryScope, "scope", "", "Scope the query")
	flag.Parse()

	initConfig()
	monitor.NewMonitor()

	if config["PG_URI"] == "" {
		log.Fatal("Make sure you set environment var for connection")
	}
}

func main() {
	log.Printf("Query scope: %s", queryScope)

	start := time.Now()
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

	svc := dynamodb.New(sess, &aws.Config{Region: aws.String("us-west-2"), Endpoint: aws.String("http://127.0.0.1:8009")})

	for _, table := range types.Table() {
		log.Printf("Init for table: %s\n", table)
		etlSession := etl.NewSession(config, table, queryScope)

		go extract.Run(etlSession, db)
		go transform.Run(etlSession)
		go loader.Run(etlSession, svc)
		etlSession.Wait()
	}

	log.Printf("ETL takes %s", time.Since(start))
}
