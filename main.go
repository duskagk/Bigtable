package main

import (
	"bigtable/sstable"
	"fmt"
	"log"
)

func main() {
	// 새로운 BigTable 인스턴스 생성
	bt,_ := sstable.NewTablet("./data")

	// 테이블 생성
	err := bt.CreateTable("users")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// 데이터 삽입
	err = bt.Put("users", "user1", "info", "name", []byte("Alice"))
	if err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}

	err = bt.Put("users", "user1", "info", "email", []byte("alice@example.com"))
	if err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}

	err = bt.Put("users", "user2", "info", "name", []byte("Bob"))
	if err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}

	// 데이터 조회
	name, err := bt.Get("users", "user1", "info", "name")
	if err != nil {
		log.Fatalf("Failed to get data: %v", err)
	}
	fmt.Printf("User1 name: %s\n", string(name))

	email, err := bt.Get("users", "user1", "info", "email")
	if err != nil {
		log.Fatalf("Failed to get data: %v", err)
	}
	fmt.Printf("User1 email: %s\n", string(email))

	name2, err := bt.Get("users", "user2", "info", "name")
	if err != nil {
		log.Fatalf("Failed to get data: %v", err)
	}
	fmt.Printf("User2 name: %s\n", string(name2))

	// 존재하지 않는 데이터 조회
	_, err = bt.Get("users", "user2", "info", "email")
	if err != nil {
		fmt.Printf("Expected error: %v\n", err)
	}
}