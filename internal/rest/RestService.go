package rest

import (
	"bigtable/internal/node"
	"encoding/json"
	"net/http"
)


type RESTService interface{
	HandleSet(w http.ResponseWriter, r *http.Request)
	HandleGet(w http.ResponseWriter, r *http.Request)
	HandleDelete(w http.ResponseWriter, r *http.Request)
	HandleCreateTable(w http.ResponseWriter, r *http.Request)
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

func (s *KVStoreService) HandleCreateTable(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Create table operation is not supported", http.StatusNotImplemented)
}