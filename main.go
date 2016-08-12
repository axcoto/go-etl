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

	//"./types"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	config map[string]string
)

func initConfig() {
	config = make(map[string]string)

	config["PG_URI"] = os.Getenv("PG_URI")
	config["PG_FETCH_LIMIT"] = os.Getenv("PG_FETCH_LIMIT")
}

func main() {
	initConfig()
	monitor.NewMonitor()

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

	extractChannel := make(chan map[string]interface{})
	transformChannel := make(chan *dynamodb.PutRequest)
	doneChannel := make(chan bool)

	go extract.PrePop("table", extractChannel, db)
	go transform.PrePop("table", extractChannel, transformChannel)
	go loader.PrePop("table", transformChannel, svc)

	<-doneChannel
	log.Printf("ETL takes %s", time.Since(start))
}
