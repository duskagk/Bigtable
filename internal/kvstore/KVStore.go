package kvstore

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
)


type KVStore struct{
	db *pebble.DB
}


type BatchOperation struct {
    Type  string      `json:"type"`  // "set" or "delete"
    Key   string      `json:"key"`
    Value interface{} `json:"value,omitempty"`  // omitempty를 사용하여 delete 작업 시 생략 가능
}

func NewKVStore(database string) (*KVStore, error) {
	db, err := pebble.Open(database, &pebble.Options{
		
	})
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}


func (s *KVStore) convertToString(value interface{}) (string, error) {
    switch v := value.(type) {
    case string:
        return v, nil
    case []byte:
        return string(v), nil
    default:
        // For other types, convert to JSON
        jsonBytes, err := json.Marshal(v)
        if err != nil {
            return "", fmt.Errorf("failed to marshal value: %v", err)
        }
        return string(jsonBytes), nil
    }
}

func (s *KVStore) Set(key string, value string) error {
	return s.db.Set([]byte(key), []byte(value), pebble.Sync)
}

func (s *KVStore) BatchOperation(operations []BatchOperation) error {
    batch := s.db.NewBatch()
    defer batch.Close()

    for _, op := range operations {
        switch op.Type {
        case "set":
            valueStr, err := s.convertToString(op.Value)
            if err != nil {
                return fmt.Errorf("failed to convert value for key %s: %v", op.Key, err)
            }
            if err := batch.Set([]byte(op.Key), []byte(valueStr), pebble.Sync); err != nil {
                return err
            }
        case "delete":
            if err := batch.Delete([]byte(op.Key), pebble.Sync); err != nil {
                return err
            }
        default:
            return fmt.Errorf("unknown operation type: %s", op.Type)
        }
    }
    return batch.Commit(pebble.Sync)
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


func (s *KVStore) ScanOffset(prefix string, offset int) (string, error) {
	if offset==0{
		return "", nil
	}
    iter, err := s.db.NewIter(&pebble.IterOptions{
        LowerBound: []byte(prefix),
    })
    if err != nil {
        return "", fmt.Errorf("failed to create iterator: %v", err)
    }
    defer iter.Close()

    iter.SeekGE([]byte(prefix))

    for i := 0; i < offset && iter.Valid(); i++ {
        if !bytes.HasPrefix(iter.Key(), []byte(prefix)) {
            return "", nil // offset이 전체 결과 수를 초과하면 빈 문자열 반환
        }
        iter.Next()
    }

    if !iter.Valid() {
        return "", nil
    }

    return string(iter.Key()), nil
}

func (s *KVStore) TotalKey(prefix string) (int,error){
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
	})
	if err != nil{
		return 0, err
	}
	defer iter.Close()

	iter.SeekGE([]byte(prefix))
	var cnt int
    for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
        if !bytes.HasPrefix(iter.Key(), []byte(prefix)) {
            break
        }
        cnt++
    }

	if err := iter.Error(); err != nil {
        return 0, err
    }

	return cnt,nil
}


func (s *KVStore) ScanValueByKey(prefix string, cursor string, limit int)([]map[string]string, string, error){
    iter, err := s.db.NewIter(&pebble.IterOptions{
        LowerBound: []byte(prefix),
    })
    if err != nil {
        return nil, "", fmt.Errorf("failed to create iterator: %v", err)
    }
    defer iter.Close()

    var results []map[string]string
    var nextCursor string

    // 커서가 제공되면 해당 위치부터 시작
    if cursor != "" {
        iter.SeekGE([]byte(cursor))
    } else {
        iter.SeekGE([]byte(prefix))
    }

    for i := 0; i < limit && iter.Valid(); i++ {
        key := iter.Key()
        value := iter.Value()

        // prefix로 시작하지 않는 키를 만나면 종료
        if !bytes.HasPrefix(key, []byte(prefix)) {
            break
        }

        results = append(results, map[string]string{
            "key":   string(key),
            "value": string(value),
        })

        iter.Next()
    }

    // 다음 페이지의 커서 설정
    if iter.Valid() && bytes.HasPrefix(iter.Key(), []byte(prefix)) {
        nextCursor = string(iter.Key())
    }

    if err := iter.Error(); err != nil {
        return nil, "", fmt.Errorf("iterator error: %v", err)
    }

    return results, nextCursor, nil
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




func (s *KVStore) Close() error {
	return s.db.Close()
}
