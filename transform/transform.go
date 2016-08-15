package transform

import (
	"../etl"
	"../types"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"time"
)

func Run(etlSession *etl.Session) {
	table := etlSession.Get("table")

	extractChannel := etlSession.ExtractChannel
	transformChannel := etlSession.TransformChannel

	defer etlSession.Wg.Done()
	defer close(transformChannel)

	for out := range extractChannel {
		//log.Printf("Received extracted value: %#v\n", out)

		if out == nil {
			log.Println("Extracted channel closed. Exit transform channel")
			continue
		}

		item := map[string]*dynamodb.AttributeValue{}

		for k, v := range out {
			if v != nil {
				switch t := v.(type) {
				case []uint8:
					if len(v.([]uint8)) > 0 {
						item[k] = &dynamodb.AttributeValue{
							S: aws.String(string(v.([]uint8))),
						}
					}
				case time.Time:
					item[k] = &dynamodb.AttributeValue{
						S: aws.String(v.(time.Time).String()),
					}
				case int64:
					item[k] = &dynamodb.AttributeValue{
						S: aws.String(fmt.Sprintf("%d", v.(int64))),
					}
				default:
					log.Fatalf("Don't know how to parse %s", t)
				}
			}
		}

		item = types.Transform(out, table, item)

		items := &dynamodb.PutRequest{
			Item: item,
		}

		log.Printf("Send to transform channel: %s", types.GetId(table, out))
		transformChannel <- items
	}
}
