package rest

import (
	"bigtable/internal/kvstore"
	"bigtable/internal/node"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)


type KVPair struct {
    Type  string      `json:"type"`
    Key   string      `json:"key"`
    Value interface{} `json:"value"`
}

type RESTService interface{
	HandleSet(w http.ResponseWriter, r *http.Request)
	HandleGet(w http.ResponseWriter, r *http.Request)
	HandleDelete(w http.ResponseWriter, r *http.Request)
	HandleRange(w http.ResponseWriter, r *http.Request)
	HandleBatch(w http.ResponseWriter, r *http.Request)
	HandleScanKey(w http.ResponseWriter,r *http.Request)
	HandleScanValueByKey(w http.ResponseWriter,r *http.Request)
	HandleScanKeysLower(w http.ResponseWriter, r *http.Request)
	HandleScanOffset(w http.ResponseWriter, r *http.Request)
	HandleTotalKey(w http.ResponseWriter, r *http.Request)
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


func (s *KVStoreService) HandleSet(w http.ResponseWriter, r *http.Request) {
    var data struct {
        Key   string      `json:"key"`
        Value interface{} `json:"value"`
    }

    if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
        log.Printf("JSON decode error: %v", err)
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }

    // value를 JSON으로 직렬화
    valueJSON, err := json.Marshal(data.Value)
    if err != nil {
        log.Printf("JSON marshal error: %v", err)
        http.Error(w, "Failed to process value", http.StatusInternalServerError)
        return
    }

    if err := s.node.Set(data.Key, string(valueJSON)); err != nil {
        log.Printf("Set error: %v", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
}

func (s *KVStoreService) HandleBatch(w http.ResponseWriter, r *http.Request) {
    var operations []KVPair
    if err := json.NewDecoder(r.Body).Decode(&operations); err != nil {
        log.Printf("JSON decode error: %v", err)
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }

    batchOps := make([]kvstore.BatchOperation, len(operations))
    for i, op := range operations {
        valueJSON, err := json.Marshal(op.Value)
        if err != nil {
            log.Printf("JSON marshal error for key %s: %v", op.Key, err)
            http.Error(w, "Failed to process value", http.StatusInternalServerError)
            return
        }

        batchOps[i] = kvstore.BatchOperation{
            Type:  op.Type,
            Key:   op.Key,
            Value: string(valueJSON),
        }
    }

    if err := s.node.BatchWrite(batchOps); err != nil {
        log.Printf("Batch operation error: %v", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "Batch operation successful"})
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

    // value가 이미 JSON 형식이라고 가정
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(value))
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

func (s *KVStoreService) HandleScanValueByKey(w http.ResponseWriter, r *http.Request) {
    prefix := r.URL.Query().Get("prefix")
    if prefix == "" {
        http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
        return
    }

    cursor := r.URL.Query().Get("cursor")
    limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
    if limit <= 0 {
        limit = 1000 // 기본값 설정
    }

    results, nextCursor, err := s.node.ScanValueByKey(prefix, cursor, limit)
    if err != nil {
        http.Error(w, "Error scanning values: "+err.Error(), http.StatusInternalServerError)
        return
    }

	formattedResults := make([]map[string]interface{}, len(results))
    for i, result := range results {
        var valueMap map[string]interface{}
        if err := json.Unmarshal([]byte(result["value"]), &valueMap); err != nil {
            http.Error(w, "Error parsing value: "+err.Error(), http.StatusInternalServerError)
            return
        }
        formattedResults[i] = map[string]interface{}{
            "key":   result["key"],
            "value": valueMap,
        }
    }

    response := struct {
        Results    []map[string]interface{}		 `json:"results"`
        NextCursor string              			`json:"nextCursor"`
    }{
        Results:    formattedResults,
        NextCursor: nextCursor,
    }

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




func (s *KVStoreService) HandleScanOffset(w http.ResponseWriter, r *http.Request){

	prefix := r.URL.Query().Get("prefix")

	if prefix == "" {
		http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
		return
	}

	offset := 1000 // Default limit
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		var err error
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	cursor, err := s.node.ScanOffset(prefix, offset)

	if err!=nil{
		http.Error(w, "Error scanning offset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cursor)
}

func (s *KVStoreService) HandleTotalKey(w http.ResponseWriter, r *http.Request){

	prefix := r.URL.Query().Get("prefix")

	if prefix == "" {
		http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
		return
	}

	totals,err := s.node.TotalKey(prefix)

	if err != nil{
		http.Error(w, "Error get total key : " + err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(totals)
}