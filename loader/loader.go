package loader

import (
	//"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
)

func PrePop(table string, transformChannel chan *dynamodb.PutRequest, svc *dynamodb.DynamoDB) {
	for {
		select {
		case items := <-transformChannel:
			params := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					table: {
						&dynamodb.WriteRequest{
							PutRequest: items,
						},
					},
				},
			}
			response, err := svc.BatchWriteItem(params)

			if err != nil {
				log.Fatal(err)
			}

			if len(response.UnprocessedItems) > 0 {
				log.Printf("Re-process %d items\n", response.UnprocessedItems)
				params = &dynamodb.BatchWriteItemInput{
					RequestItems: response.UnprocessedItems,
				}
				continue
			} else {
				break
			}
		}
	}
}
