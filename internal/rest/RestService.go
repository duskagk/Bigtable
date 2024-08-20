package rest

import (
	"bigtable/internal/kvstore"
	"bigtable/internal/node"
	"encoding/json"
	"net/http"
)


type RESTService interface{
	HandleSet(w http.ResponseWriter, r *http.Request)
	HandleGet(w http.ResponseWriter, r *http.Request)
	HandleDelete(w http.ResponseWriter, r *http.Request)
	HandleRange(w http.ResponseWriter, r *http.Request)
	HandleBatchOperation(w http.ResponseWriter, r *http.Request)
}


type KVStoreService struct {
	node *node.KVNode
}

func NewKVStoreService(node *node.KVNode) *KVStoreService {
	return &KVStoreService{node: node}
}

func (s *KVStoreService) HandleSet(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := s.node.Set(key, value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *KVStoreService) HandleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	value, err := s.node.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"value": value})
}

func (s *KVStoreService) HandleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	if err := s.node.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *KVStoreService) HandleRange(w http.ResponseWriter, r *http.Request){
	startKey := r.URL.Query().Get("startKey")
	endKey := r.URL.Query().Get("endKey")
	if startKey == "" || endKey == "" {
		http.Error(w, "Both startKey and endKey are required", http.StatusBadRequest)
		return
	}

	result, err := s.node.RangeQuery(startKey, endKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(result)
}

func (s *KVStoreService) HandleBatchOperation(w http.ResponseWriter, r *http.Request){
	var operations []kvstore.BatchOperation
	if err := json.NewDecoder(r.Body).Decode(&operations); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.node.BatchWrite(operations); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}