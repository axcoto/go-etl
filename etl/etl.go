package etl

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"sync"
)

type Session struct {
	ExtractChannel   chan map[string]interface{}
	TransformChannel chan *dynamodb.PutRequest
	Wg               *sync.WaitGroup

	params map[string]string
}

func NewSession(table, scope string) *Session {
	etlSession := Session{
		ExtractChannel:   make(chan map[string]interface{}),
		TransformChannel: make(chan *dynamodb.PutRequest),
		Wg:               &sync.WaitGroup{},
		params:           make(map[string]string),
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
