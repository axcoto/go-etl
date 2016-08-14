package loader

import (
	//"github.com/aws/aws-sdk-go/aws"
	"../etl"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
)

func Run(etlSession *etl.Session, svc *dynamodb.DynamoDB) {
	table := etlSession.Get("table")
	transformChannel := etlSession.TransformChannel

	defer etlSession.Wg.Done()

	for items := range transformChannel {
		//select {
		//case items := <-transformChannel:
		params := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				table: {
					&dynamodb.WriteRequest{
						PutRequest: items,
					},
				},
			},
		}

		log.Println("Flush to DynamoDB")
		response, err := svc.BatchWriteItem(params)

		if err != nil {
			log.Fatal(err)
		}

		for {
			if len(response.UnprocessedItems) > 0 {
				log.Printf("Re-process %d items\n", response.UnprocessedItems)
				params = &dynamodb.BatchWriteItemInput{
					RequestItems: response.UnprocessedItems,
				}
			} else {
				break
			}
		}
		//}
	}
}
