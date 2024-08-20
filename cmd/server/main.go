package main

import (
	"bigtable/internal/node"
	"bigtable/internal/rest"
	"log"
)

func main() {
	
	kvNode, err := node.NewKVNode("kv_data")

	if err !=nil{
		log.Fatalf("Failed to create KVnode: %v", err)
	}

	defer kvNode.Close()

	kvService := rest.NewKVStoreService(kvNode)

	server := rest.NewServer(kvService)

	if err := server.Start(":6195"); err != nil{
		log.Fatalf("Server failed to start : %v", err)
	}

}