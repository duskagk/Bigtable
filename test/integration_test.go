package test

import (
	"bigtable/internal/kvstore"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/rand"
)




func TestInsertAndRandomGet(t *testing.T) {
	// Initialize the KVNode (assuming it wraps around KVStore)
	node, err := kvstore.NewKVStore("testdb")
	if err != nil {
		t.Fatalf("Failed to create KVNode: %v", err)
	}
	defer node.Close()

	// Insert a large number of key-value pairs
	var wg sync.WaitGroup
	numGoroutines := 10
	numEntriesPerRoutine := 1000

	// Start 10 Goroutines to insert data
	for j := 0; j < numGoroutines; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			start := j * numEntriesPerRoutine
			end := (j + 1) * numEntriesPerRoutine
			for i := start; i < end; i++ {
				key := "key" + strconv.Itoa(i)
				value := "value" + strconv.Itoa(i)
				if err := node.Set(key, value); err != nil {
					log.Printf("Failed to set key %s: %v", key, err)
				}
			}
		}(j)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Printf("TEST is middle")
	// Perform random Get operations
	rand.Seed(uint64(time.Now().UnixNano()))
	for i := 0; i < 1000; i++ {
		randomKey := "key" + strconv.Itoa(rand.Intn(10000))
		_, err := node.Get(randomKey)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", randomKey, err)
		}
	}

	log.Printf("TEST is end")
}

func TestBatchOperations(t *testing.T) {
	// Initialize the KVNode
	node, err := kvstore.NewKVStore("testdb_batch")
	if err != nil {
		t.Fatalf("Failed to create KVNode: %v", err)
	}
	defer node.Close()

	// Prepare a batch of operations
	numEntries := 10000
	operations := make([]kvstore.BatchOperation, 0, numEntries)

	for i := 0; i < numEntries; i++ {
		key := "batchKey" + strconv.Itoa(i)
		value := "batchValue" + strconv.Itoa(i)
		op := kvstore.BatchOperation{
			Type:  "set",
			Key:   key,
			Value: value,
		}
		operations = append(operations, op)
	}

	// Execute batch operations
	if err := node.BatchOperation(operations); err != nil {
		t.Fatalf("Batch operation failed: %v", err)
	}

	// Verify batch operations
	for i := 0; i < 1000; i++ {
		key := "batchKey" + strconv.Itoa(rand.Intn(numEntries))
		_, err := node.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s after batch operation: %v", key, err)
		}
	}
}