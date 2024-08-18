// internal/bigtable/tablet.go

package bigtable

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)


type Tablet struct {
    tableName  string
    startKey   string
    endKey     string
    data       map[string][]byte
    mutex      sync.RWMutex
    lastFlush  time.Time
    flushSize  int
    flushCount int
}


func NewTablet(tableName, startKey, endKey string) *Tablet {
    return NewTabletWithConfig(tableName, startKey, endKey, 1024)
}

func NewTabletWithConfig(tableName, startKey, endKey string, flushSize int) *Tablet {
    return &Tablet{
        tableName:  tableName,
        startKey:   startKey,
        endKey:     endKey,
        data:       make(map[string][]byte),
        lastFlush:  time.Now(),
        flushSize:  flushSize,
        flushCount: 0,
    }
}

func (t *Tablet) NeedsToSplit() bool {
    return len(t.data) > t.flushSize*2  // Example condition for splitting
}

func (t *Tablet) Read(row string) ([]byte, error) {
    t.mutex.RLock()
    defer t.mutex.RUnlock()
    
    if data, exists := t.data[row]; exists {
        return data, nil
    }
    
    // If not in memory, try to read from disk
    return t.readFromDisk(row)
}

func (t *Tablet) Write(row string, data []byte) error {
    t.mutex.Lock()
    defer t.mutex.Unlock()
    
    t.data[row] = data
    
    if len(t.data) >= t.flushSize {
        log.Printf("Flush size reached (%d entries), initiating flush...", len(t.data))
        return t.flush()
    }
    
    return nil
}

func (t *Tablet) flush() error {
    log.Println("Starting flush operation...")
    
    t.flushCount++
    fileName := fmt.Sprintf("%s_%s_%s_%d.gob", t.tableName, t.startKey, t.endKey, t.flushCount)
    log.Printf("Attempting to create file: %s", fileName)

    file, err := os.Create(fileName)
    if err != nil {
        log.Printf("Failed to create file %s: %v", fileName, err)
        return fmt.Errorf("failed to create file %s: %v", fileName, err)
    }
    defer file.Close()
    
    log.Printf("Successfully created file: %s", fileName)

    encoder := gob.NewEncoder(file)
    if err := encoder.Encode(t.data); err != nil {
        log.Printf("Failed to encode data to file %s: %v", fileName, err)
        return fmt.Errorf("failed to encode data to file %s: %v", fileName, err)
    }
    
    log.Printf("Successfully encoded %d entries to file: %s", len(t.data), fileName)

    t.data = make(map[string][]byte)
    t.lastFlush = time.Now()
    
    log.Println("Flush operation completed successfully")
    return nil
}


func (t *Tablet) readFromDisk(row string) ([]byte, error) {
    fileName := filepath.Join("data", t.startKey + "_" + t.endKey + ".gob")
    file, err := os.Open(fileName)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var diskData map[string][]byte
    decoder := gob.NewDecoder(file)
    if err := decoder.Decode(&diskData); err != nil {
        return nil, err
    }

    if data, exists := diskData[row]; exists {
        return data, nil
    }
    return nil, errors.New("row not found")
}

func (t *Tablet) FindMidKey() string {
    keys := make([]string, 0, len(t.data))
    for k := range t.data {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    return keys[len(keys)/2]
}