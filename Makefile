


all: help

help: ## display help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

clear:
	clear

install: ## build library
	go install ./...

test: ## test workerpool
	go test -race ./...

short: ## run short tests only
	go test -v -race -short ./...

re: clear install short ## rebuild and run short tests

