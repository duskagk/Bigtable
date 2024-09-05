package node

import (
	"bigtable/internal/kvstore"
	"sync"
)

type KVNode struct {
	store *kvstore.KVStore
	mu    sync.RWMutex
}

func NewKVNode(database string) (*KVNode, error) {
	store, err := kvstore.NewKVStore(database)
	if err != nil {
		return nil, err
	}
	return &KVNode{store: store}, nil
}

func (n *KVNode) Set(key string, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.Set(key, value)
}

func (n *KVNode) Get(key string) (string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.Get(key)
}

func (n *KVNode) RangeQuery(startKey, endKey string) (map[string]string, error){
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.RangeQuery(startKey,endKey)
}

func (n *KVNode) ScanKey(prefix string, cursor string, limit int) ([]string, string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.ScanKey(prefix,cursor, limit)
}

func (n *KVNode) ScanKeysLower(prefix string, maxTimestamp int64, cursor string, limit int) ([]string, string, error){
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.ScanKeysLower(prefix,maxTimestamp,cursor, limit)
}

func (n *KVNode) ScanValueByKey(prefix string, cursor string, limit int) ([]map[string]string, string, error) {
    n.mu.RLock()
    defer n.mu.RUnlock()
    return n.store.ScanValueByKey(prefix, cursor, limit)
}


func (n *KVNode) BatchWrite(operations []kvstore.BatchOperation) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.BatchOperation(operations)
}

func (n *KVNode) Delete(key string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.Delete(key)
}

func (n *KVNode) ScanOffset(prefix string, offset int) (string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.ScanOffset(prefix, offset)
}

func (n *KVNode) TotalKey(prefix string) (int,error){
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store.TotalKey(prefix)
}


func (n *KVNode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.Close()
}