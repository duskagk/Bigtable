package bigtable

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MAX_ENTRIES_PER_FILE = 1000
	MEMTABLE_SIZE        = 1000
)

type Cell struct {
	Value     []byte
	Timestamp time.Time
}

type Row struct {
	Key   string
	Cells map[string][]Cell
}

type MemTable struct {
	rows map[string]Row
	size int
}

type SSTable struct {
	fileName string
	minKey   string
	maxKey   string
}

type Table struct {
	Name      string
	memTable  *MemTable
	ssTables  []SSTable
	mutex     sync.RWMutex
}

type BigTable struct {
	Tables  map[string]*Table
	dataDir string
}

func NewBigTable(dataDir string) (*BigTable, error) {
	bt := &BigTable{
		Tables:  make(map[string]*Table),
		dataDir: dataDir,
	}
	
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}
	
	err = bt.loadFromDisk()
	if err != nil {
		return nil, fmt.Errorf("failed to load data from disk: %v", err)
	}
	
	return bt, nil
}

func (bt *BigTable) loadFromDisk() error {
	fmt.Println("Starting loadFromDisk")
	files, err := os.ReadDir(bt.dataDir)
	if err != nil {
		return fmt.Errorf("error reading data directory: %v", err)
	}

	for _, file := range files {
		fmt.Printf("Checking directory: %s\n", file.Name())
		if file.IsDir() {
			tableName := file.Name()
			fmt.Printf("Loading table: %s\n", tableName)
			err := bt.loadTableFromDisk(tableName)
			if err != nil {
				return fmt.Errorf("error loading table %s: %v", tableName, err)
			}
		}
	}
	fmt.Println("Finished loadFromDisk")
	return nil
}

func (bt *BigTable) loadTableFromDisk(tableName string) error {
	table := &Table{
		Name: tableName,
		memTable: &MemTable{
			rows: make(map[string]Row),
		},
	}

	tableDir := filepath.Join(bt.dataDir, tableName)
	files, err := os.ReadDir(tableDir)
	if err != nil {
		return fmt.Errorf("error reading table directory: %v", err)
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), tableName+"_") && strings.HasSuffix(file.Name(), ".gob") {
			filePath := filepath.Join(tableDir, file.Name())
			f, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("error opening file %s: %v", filePath, err)
			}
			defer f.Close()

			var rows []Row
			decoder := gob.NewDecoder(f)
			err = decoder.Decode(&rows)
			if err != nil {
				return fmt.Errorf("error decoding data from file %s: %v", filePath, err)
			}

			// Merge rows into memTable
			for _, row := range rows {
				existingRow, exists := table.memTable.rows[row.Key]
				if !exists {
					table.memTable.rows[row.Key] = row
				} else {
					// Merge cells, keeping the most recent version
					for colFamily, cells := range row.Cells {
						existingRow.Cells[colFamily] = cells
					}
					table.memTable.rows[row.Key] = existingRow
				}
			}

			// Create SSTable entry
			sstable := SSTable{
				fileName: filePath,
				minKey:   rows[0].Key,
				maxKey:   rows[len(rows)-1].Key,
			}
			table.ssTables = append(table.ssTables, sstable)
		}
	}

	bt.Tables[tableName] = table
	return nil
}

func (bt *BigTable) CreateTable(name string) error {
	if _, exists := bt.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}
	bt.Tables[name] = &Table{
		Name: name,
		memTable: &MemTable{
			rows: make(map[string]Row),
		},
	}
	return os.MkdirAll(filepath.Join(bt.dataDir, name), 0755)
}

func (bt *BigTable) Put(tableName, rowKey, columnFamily, column string, value []byte) error {
	table, exists := bt.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	table.mutex.Lock()
	defer table.mutex.Unlock()

	cell := Cell{
		Value:     value,
		Timestamp: time.Now(),
	}

	row, exists := table.memTable.rows[rowKey]
	if !exists {
		row = Row{
			Key:   rowKey,
			Cells: make(map[string][]Cell),
		}
	}

	cellKey := columnFamily + ":" + column
	row.Cells[cellKey] = append(row.Cells[cellKey], cell)
	table.memTable.rows[rowKey] = row
	table.memTable.size++

	if table.memTable.size >= MEMTABLE_SIZE {
		err := bt.flushMemTable(table)
		if err != nil {
			return fmt.Errorf("failed to flush memtable: %v", err)
		}
	}

	return nil
}

func (bt *BigTable) flushMemTable(table *Table) error {
	if len(table.memTable.rows) == 0 {
		return nil
	}

	rows := make([]Row, 0, len(table.memTable.rows))
	for _, row := range table.memTable.rows {
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Key < rows[j].Key
	})

	fileName := fmt.Sprintf("%s_%d.gob", table.Name, time.Now().UnixNano())
	filePath := filepath.Join(bt.dataDir, table.Name, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file %s: %v", filePath, err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(rows)
	if err != nil {
		return fmt.Errorf("error encoding data to file %s: %v", filePath, err)
	}

	sstable := SSTable{
		fileName: filePath,
		minKey:   rows[0].Key,
		maxKey:   rows[len(rows)-1].Key,
	}

	table.ssTables = append(table.ssTables, sstable)
	table.memTable = &MemTable{
		rows: make(map[string]Row),
	}

	return nil
}

func (bt *BigTable) Get(tableName, rowKey, columnFamily, column string) ([]byte, error) {
	table, exists := bt.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table.mutex.RLock()
	defer table.mutex.RUnlock()

	fmt.Printf("Searching for key: %s in table: %s\n", rowKey, tableName)

	// Check memTable first
	if row, exists := table.memTable.rows[rowKey]; exists {
		fmt.Printf("Found key %s in memTable\n", rowKey)
		cellKey := columnFamily + ":" + column
		if cells, exists := row.Cells[cellKey]; exists && len(cells) > 0 {
			return cells[len(cells)-1].Value, nil
		}
	}

	// Check SSTables
	cellKey := columnFamily + ":" + column
	for i := len(table.ssTables) - 1; i >= 0; i-- {
		sstable := table.ssTables[i]
		if rowKey >= sstable.minKey && rowKey <= sstable.maxKey {
			fmt.Printf("Searching in SSTable: %s\n", sstable.fileName)
			value, err := bt.getFromSSTable(sstable, rowKey, cellKey)
			if err == nil {
				return value, nil
			}
		}
	}

	return nil, fmt.Errorf("row %s or cell %s:%s does not exist", rowKey, columnFamily, column)
}

func (bt *BigTable) getFromSSTable(sstable SSTable, rowKey, cellKey string) ([]byte, error) {
	file, err := os.Open(sstable.fileName)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", sstable.fileName, err)
	}
	defer file.Close()

	var rows []Row
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&rows)
	if err != nil {
		return nil, fmt.Errorf("error decoding data from file %s: %v", sstable.fileName, err)
	}

	index := sort.Search(len(rows), func(i int) bool {
		return rows[i].Key >= rowKey
	})

	if index < len(rows) && rows[index].Key == rowKey {
		cells, exists := rows[index].Cells[cellKey]
		if exists && len(cells) > 0 {
			return cells[len(cells)-1].Value, nil
		}
	}

	return nil, fmt.Errorf("row %s or cell %s does not exist in SSTable", rowKey, cellKey)
}