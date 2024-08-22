package kvstore

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble"
)


type KVStore struct{
	db *pebble.DB
}


type BatchOperation struct {
	Type  string // "set" or "delete"
	Key   string
	Value string // only used for "set" operations
}

func NewKVStore(database string) (*KVStore, error) {
	db, err := pebble.Open(database, &pebble.Options{
		
	})
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}



func (s *KVStore) Set(key string, value string) error {
	return s.db.Set([]byte(key), []byte(value), pebble.Sync)
}

func (s *KVStore) Get(key string) (string, error) {
	value, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	defer closer.Close()
	return string(value), nil
}

func (s *KVStore) RangeQuery(startKey, endKey string) (map[string]string,error){
	iter,err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startKey),
		UpperBound: []byte(endKey),
	})

	if err!=nil{
		return nil, err
	}

	defer iter.Close()

	result := make(map[string]string)

	for iter.First(); iter.Valid();iter.Next(){
		key := string(iter.Key())
		value := string(iter.Value())
		result[key] = value
	}

	if err := iter.Error();err != nil{
		return nil,err
	}
	
	return result, nil

}

func (s *KVStore) ScanKey(prefix string, cursor string, limit int) ([]string, string, error) {
	iter,err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
	})

	if err != nil {
		return nil, "", err
	}

	defer iter.Close()

	var keys []string
	var nextCursor string

	if cursor != "" {
		iter.SeekGE([]byte(cursor))
	} else {
		iter.SeekGE([]byte(prefix))
	}

	for i := 0; i < limit && iter.Valid(); i++ {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte(prefix)) {
			break
		}
		keys = append(keys, string(key))
		iter.Next()
	}

	if iter.Valid() && bytes.HasPrefix(iter.Key(), []byte(prefix)) {
		nextCursor = string(iter.Key())
	}

	if err := iter.Error(); err != nil {
		return nil, "", err
	}

	return keys, nextCursor, nil
}


func (s *KVStore) ScanKeysLower(prefix string, maxTimestamp int64, cursor string, limit int) ([]string, string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
	})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()

	var keys []string
	var nextCursor string

	if cursor != "" {
		iter.SeekGE([]byte(cursor))
	} else {
		iter.SeekGE([]byte(prefix))
	}

	for i := 0; i < limit && iter.Valid(); i++ {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte(prefix)) {
			break
		}

		var timestamp int64
		_, err := fmt.Sscanf(string(key), "%s%d:", &prefix, &timestamp)
		if err != nil {
			iter.Next()
			continue // Skip keys that don't match the expected format
		}

		if timestamp > maxTimestamp {
			break // Stop if we've reached a timestamp greater than maxTimestamp
		}

		keys = append(keys, string(key))
		iter.Next()
	}

	if iter.Valid() && bytes.HasPrefix(iter.Key(), []byte(prefix)) {
		nextCursor = string(iter.Key())
	}

	if err := iter.Error(); err != nil {
		return nil, "", err
	}

	return keys, nextCursor, nil
}



func (s *KVStore) Delete(key string) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}


func (s *KVStore) BatchOperation(operations []BatchOperation)error{
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, op := range operations{
		switch op.Type{
		case "set" :
			if err := batch.Set([]byte(op.Key), []byte(op.Value), pebble.Sync);err != nil{
				return err
			}
		case "delete":
			if err := batch.Delete([]byte(op.Key), pebble.Sync); err != nil{
				return err
			}
		}
	}
	return batch.Commit(pebble.Sync)
}


func (s *KVStore) Close() error {
	return s.db.Close()
}
