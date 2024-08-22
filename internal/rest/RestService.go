package rest

import (
	"bigtable/internal/kvstore"
	"bigtable/internal/node"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)


type RESTService interface{
	HandleSet(w http.ResponseWriter, r *http.Request)
	HandleGet(w http.ResponseWriter, r *http.Request)
	HandleDelete(w http.ResponseWriter, r *http.Request)
	HandleRange(w http.ResponseWriter, r *http.Request)
	HandleBatchOperation(w http.ResponseWriter, r *http.Request)
	HandleScanKey(w http.ResponseWriter,r *http.Request)
	HandleScanKeysLower(w http.ResponseWriter, r *http.Request)
}



type ScanKeyResponse struct {
	Keys       []string `json:"keys"`
	NextCursor string   `json:"nextCursor"`
}

type KVStoreService struct {
	node *node.KVNode
}

func NewKVStoreService(node *node.KVNode) *KVStoreService {
	return &KVStoreService{node: node}
}

func (s *KVStoreService) setSingle(key string, value interface{}) error {
	valueStr, err := s.convertToString(value)
	if err != nil {
		return err
	}
	return s.node.Set(key, valueStr)
}

func (s *KVStoreService) convertToString(value interface{}) (string, error) {
	var valueStr string

	switch v := value.(type) {
	case string:
		valueStr = v
	case float64:
		valueStr = fmt.Sprintf("%f", v)
	case bool:
		valueStr = fmt.Sprintf("%t", v)
	default:
		// JSON 문자열로 변환
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		valueStr = string(valueJSON)
	}

	return valueStr, nil
}

func (s *KVStoreService) HandleSet(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
    if err != nil {
        log.Printf("Error reading request body: %v", err)
        http.Error(w, "Error reading request body", http.StatusBadRequest)
        return
    }
    
    log.Printf("Raw request body: %s", string(body))

    contentType := r.Header.Get("Content-Type")
    log.Printf("Content-Type: %s", contentType)


	log.Printf("Current Request body %v",r.Body)
    var data map[string]interface{}
    err = json.Unmarshal(body, &data)
    if err != nil {
        log.Printf("JSON unmarshal error: %v", err)
        http.Error(w, fmt.Sprintf("JSON unmarshal error: %v", err), http.StatusBadRequest)
        return
    }
	log.Printf("Decoded data: %+v", data)
	if len(data) == 1 {
		for key, value := range data {
			if err := s.setSingle(key, value); err != nil {
				log.Printf("Set error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	var operations []kvstore.BatchOperation
	for key, value := range data {
		valueStr, err := s.convertToString(value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		operations = append(operations, kvstore.BatchOperation{
			Type:  "set",
			Key:   key,
			Value: valueStr,
		})
	}

	if err := s.node.BatchWrite(operations); err != nil {
		log.Printf("BatchOperation error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
		log.Printf("NO DATA FOUND")
		http.Error(w, err.Error(), http.StatusNoContent)
		return
	}

	json.NewEncoder(w).Encode(value)
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


func (s *KVStoreService) HandleScanKey(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
		return
	}

	cursor := r.URL.Query().Get("cursor")

	limit := 1000 // Default limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	// Call ScanKey
	keys, nextCursor, err := s.node.ScanKey(prefix, cursor, limit)
	if err != nil {
		http.Error(w, "Error scanning keys: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare response
	response := ScanKeyResponse{
		Keys:       keys,
		NextCursor: nextCursor,
	}

	// Send JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *KVStoreService) HandleScanKeysLower(w http.ResponseWriter, r *http.Request){

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
		return
	}

	maxTimestamp := time.Now().Unix()
	if tsStr := r.URL.Query().Get("maxTimestamp"); tsStr != "" {
		var err error
		maxTimestamp, err = strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid maxTimestamp parameter", http.StatusBadRequest)
			return
		}
	}

	cursor := r.URL.Query().Get("cursor")

	limit := 1000 // Default limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	// Call ScanKeysLower
	keys, nextCursor, err := s.node.ScanKeysLower(prefix, maxTimestamp, cursor, limit)
	if err != nil {
		http.Error(w, "Error scanning keys: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare response
	response := ScanKeyResponse{
		Keys:       keys,
		NextCursor: nextCursor,
	}

	// Send JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

}