package kvstore

import (
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





