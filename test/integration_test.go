package test

import (
	"bigtable/internal/kvstore"
	"fmt"
	"testing"
)


var store *kvstore.KVStore

func TestMain(m *testing.M) {
	var err error
	store, err = kvstore.NewKVStore()
	if err != nil {
		panic(fmt.Sprintf("Failed to create KVStore: %v", err))
	}
	defer store.Close()

	m.Run()
}

func BenchmarkSequentialWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value)
		if err != nil {
			b.Fatalf("Failed to set key-value: %v", err)
		}
	}
}

