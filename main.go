package main

import (
	"bigtable/internal/bigtable"
	"log"
)

func main() {
    log.Println("Starting BigTable test...")

    bt := bigtable.NewBigTable()

    // Test creating a table
    err := bt.CreateTable("users")
    if err != nil {
        log.Fatalf("Failed to create table: %v", err)
    }
    log.Println("Table 'users' created successfully")

    // Test writing data
    err = bt.Write("users", "user1", []byte("John Doe"))
    if err != nil {
        log.Fatalf("Failed to write data: %v", err)
    }
    log.Println("Data written successfully")

    // Test reading data
    data, err := bt.Read("users", "user1")
    if err != nil {
        log.Fatalf("Failed to read data: %v", err)
    }
    log.Printf("Read data: %s", string(data))

    // Test reading non-existent data
    _, err = bt.Read("users", "user2")
    if err != nil {
        log.Printf("Expected error when reading non-existent data: %v", err)
    } else {
        log.Fatalf("Expected an error when reading non-existent data, but got none")
    }

    // Test creating an existing table
    err = bt.CreateTable("users")
    if err != nil {
        log.Printf("Expected error when creating existing table: %v", err)
    } else {
        log.Fatalf("Expected an error when creating an existing table, but got none")
    }

    log.Println("All tests completed successfully")
}