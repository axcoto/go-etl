package etl

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strconv"
	"sync"
)

type Session struct {
	ExtractChannel   chan map[string]interface{}
	TransformChannel chan *dynamodb.PutRequest
	Wg               *sync.WaitGroup

	// TODO struct
	config map[string]string
	params map[string]string
}

func NewSession(config map[string]string, table, scope string) *Session {
	batchWrite, _ := strconv.ParseInt(config["DYNAMODB_BATCH_WRITE"], 10, 0)

	etlSession := Session{
		ExtractChannel:   make(chan map[string]interface{}),
		TransformChannel: make(chan *dynamodb.PutRequest, batchWrite),
		Wg:               &sync.WaitGroup{},
		params:           make(map[string]string),
		config:           config,
	}

	etlSession.SetParam("table", table)
	etlSession.SetParam("scope", scope)

	etlSession.Start()

	return &etlSession
}

func (s *Session) SetParam(key, value string) {
	s.params[key] = value
}

func (s *Session) Get(key string) string {
	return s.params[key]
}

func (s *Session) Start() {
	s.Wg.Add(3)
}

func (s *Session) Wait() {
	s.Wg.Wait()
}

func (s *Session) Config(key string) string {
	return s.config[key]
}
