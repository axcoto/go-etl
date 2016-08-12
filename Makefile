setup:
	cd schemas; ./setup.sh
.PHONY: setup

list_table:
	aws dynamodb list-tables --endpoint-url http://127.0.0.1:8009
.PHONY: list_table
