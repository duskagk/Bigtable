package node

import (
	"bigtable/internal/kvstore"
	"sync"
)

type KVNode struct {
	store *kvstore.KVStore
	mu    sync.RWMutex
}

func NewKVNode() (*KVNode, error) {
	store, err := kvstore.NewKVStore()
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

func (n *KVNode) Delete(key string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.Delete(key)
}

func (n *KVNode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.Close()
}