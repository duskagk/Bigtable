.PHONY: build run-master run-slave clean

build:
	go build -o bin/dfs main.go

run-master: build
	./bin/dfs -type=master -port=8000

run-slave: build
	./bin/dfs -type=slave -port=8001 -master=localhost:8000

clean:
	rm -rf bin
	rm -rf slave_storage