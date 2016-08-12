package transform

import (
	//"../types"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"time"
)

func PrePop(table string, extractChannel chan map[string]interface{}, transformChannel chan *dynamodb.PutRequest) {
	for {
		select {
		case out := <-extractChannel:
			log.Printf("%#v\n", out)
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

			switch table {
			case "table":
				item["extra_key"] = &dynamodb.AttributeValue{
					N: aws.String(fmt.Sprintf("%d", out["key"].(time.Time).Unix())),
				}
			}
			items := &dynamodb.PutRequest{
				Item: item,
			}

			transformChannel <- items
		}
	}
}
