package transform

import (
	"../etl"
	"../types"
	//"fmt"
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
		//select {
		//case out := <-extractChannel:
		log.Printf("Recive extracted value: %#v\n", out)

		if out == nil {
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
				default:
					log.Fatalf("Don't know how to parse %s", t)
				}
			}
		}

		item = types.Transform(out, table, item)

		items := &dynamodb.PutRequest{
			Item: item,
		}

		log.Println("Send to transform channel")
		transformChannel <- items
		//}
	}
}
