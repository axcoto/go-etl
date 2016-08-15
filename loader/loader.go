package loader

import (
	//"github.com/aws/aws-sdk-go/aws"
	"../etl"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"strconv"
	"sync"
)

func flush(svc *dynamodb.DynamoDB, params *dynamodb.BatchWriteItemInput) {
	for {
		//log.Printf("Flush to dynamo %#v\n", params)
		response, err := svc.BatchWriteItem(params)
		//log.Printf("Done flush to dynamo %#v\n", params)

		if err != nil {
			log.Fatal(err)
		}

		if len(response.UnprocessedItems) > 0 {
			log.Printf("Re-process %d items\n", response.UnprocessedItems)
			params = &dynamodb.BatchWriteItemInput{
				RequestItems: response.UnprocessedItems,
			}
			response, err = svc.BatchWriteItem(params)
		} else {
			break
		}
	}
}

func StartWorker() {
}

func Run(etlSession *etl.Session, svc *dynamodb.DynamoDB) {
	table := etlSession.Get("table")
	transformChannel := etlSession.TransformChannel

	workerCount, _ := strconv.Atoi(etlSession.Config("DYNAMODB_PARALLEL_FLUSH"))
	workerWait := sync.WaitGroup{}
	workerWait.Add(workerCount)

	loadChannel := make(chan *dynamodb.BatchWriteItemInput)
	for worker := 0; worker <= workerCount; worker++ {
		go func() {
			for payload := range loadChannel {
				flush(svc, payload)
			}
			workerWait.Done()
		}()
	}

	defer func() {
		close(loadChannel)
		workerWait.Wait()
		etlSession.Wg.Done()
	}()

	for items := range transformChannel {
		params := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				table: {
					&dynamodb.WriteRequest{
						PutRequest: items,
					},
				},
			},
		}

		//log.Println("Flush to worker", items)
		loadChannel <- params
	}
}
