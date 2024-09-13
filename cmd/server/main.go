package main

import (
	"bigtable/internal/node"
	"bigtable/internal/rest"
	"flag"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	
	dbPath := flag.String("db", "kv_data", "Path to the database directory")

	port := flag.Int("port", 6195, "Port number for the server")
	flag.Parse()


	absDbPath, err := filepath.Abs(*dbPath)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	kvNode, err := node.NewKVNode(absDbPath)

	if err !=nil{
		log.Fatalf("Failed to create KVnode: %v", err)
	}

	defer kvNode.Close()

	kvService := rest.NewKVStoreService(kvNode)

	server := rest.NewServer(kvService)
	address := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server on %s with database at %s", address, absDbPath)
	if err := server.Start(address); err != nil{
		log.Fatalf("Server failed to start : %v", err)
	}

}