package loader

import (
	//"github.com/aws/aws-sdk-go/aws"
	"../etl"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"strconv"
)

func flush(svc *dynamodb.DynamoDB, params *dynamodb.BatchWriteItemInput) {
	for {
		//log.Printf("Flush to dynamo %#v\n", params)
		response, err := svc.BatchWriteItem(params)

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

func Run(etlSession *etl.Session, svc *dynamodb.DynamoDB) {
	table := etlSession.Get("table")
	transformChannel := etlSession.TransformChannel

	defer etlSession.Wg.Done()

	loadChannel := make(chan *dynamodb.BatchWriteItemInput)
	workerCount, _ := strconv.Atoi(etlSession.Config("DYNAMODB_PARALLEL_FLUSH"))
	for worker := 0; worker <= workerCount; worker++ {
		go func() {
			for payload := range loadChannel {
				flush(svc, payload)
			}
		}()
	}

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

		loadChannel <- params
	}
	close(loadChannel)
}
