package kvstore

import (
	"github.com/cockroachdb/pebble"
)


type KVStore struct{
	db *pebble.DB
}


func NewKVStore() (*KVStore, error) {
	db, err := pebble.Open("kvstore-data", &pebble.Options{})
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

func (s *KVStore) Delete(key string) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}


func (s *KVStore) Close() error {
	return s.db.Close()
}
