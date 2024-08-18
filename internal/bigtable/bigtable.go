package bigtable

import (
	"errors"
	"fmt"
	"sync"
)

type BigTable struct {
    tables map[string]*Table
    mutex  sync.RWMutex
}

func NewBigTable() *BigTable {
    return &BigTable{
        tables: make(map[string]*Table),
    }
}

func (bt *BigTable) CreateTable(name string) error {
    bt.mutex.Lock()
    defer bt.mutex.Unlock()

    if _, exists := bt.tables[name]; exists {
        return fmt.Errorf("table already exists")
    }

    bt.tables[name] = NewTable(name)
    return nil
}

func (bt *BigTable) Write(tableName, row string, data []byte) error {
    bt.mutex.RLock()
    table, exists := bt.tables[tableName]
    bt.mutex.RUnlock()

    if !exists {
        return fmt.Errorf("table not found")
    }

    return table.Write(row, data)
}



func (bt *BigTable) Read(tableName, row string) ([]byte, error) {
    bt.mutex.RLock()
    table, exists := bt.tables[tableName]
    bt.mutex.RUnlock()

    if !exists {
        return nil, errors.New("table not found")
    }

    return table.Read(row)
}