VERSION := $(shell sh -c 'git describe --always --tags')

setup:
	cd schemas; ./setup.sh
.PHONY: setup

list_table:
	aws dynamodb list-tables --endpoint-url http://127.0.0.1:8009
.PHONY: list_table

build:
	go build  -ldflags "-X main.version=$(VERSION)" main.go
	chmod 755 main
	mv main bin/etl
