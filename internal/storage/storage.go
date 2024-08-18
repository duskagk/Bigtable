package storage

import "errors"


type Storage interface {
    Write(key string, value []byte) error
    Read(key string) ([]byte, error)
}

// In-memory storage implementation
type MemoryStorage struct {
    data map[string][]byte
}

func NewMemoryStorage() *MemoryStorage {
    return &MemoryStorage{
        data: make(map[string][]byte),
    }
}

func (ms *MemoryStorage) Write(key string, value []byte) error {
    ms.data[key] = value
    return nil
}

func (ms *MemoryStorage) Read(key string) ([]byte, error) {
    value, exists := ms.data[key]
    if !exists {
        return nil, errors.New("key not found")
    }
    return value, nil
}