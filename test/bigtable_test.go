package test

import (
	"bigtable/internal/bigtable"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBigTableFlush(t *testing.T) {
    log.Println("Starting TestBigTableFlush...")
    
    // Get the current working directory
    cwd, err := os.Getwd()
    if err != nil {
        t.Fatalf("Failed to get current working directory: %v", err)
    }
    log.Printf("Current working directory: %s", cwd)

    bt := bigtable.NewBigTable()

    tableName := "flushtest"
    err = bt.CreateTable(tableName)
    if err != nil {
        t.Fatalf("Failed to create table: %v", err)
    }
    log.Printf("Created '%s' table", tableName)

    // Write data until flush is triggered
    dataSize := 10 // Smaller data size for quicker testing
    totalWrites := 1100 // Writing more than flush size to ensure flush
    log.Printf("Writing %d entries of %d bytes each...", totalWrites, dataSize)
    
    for i := 0; i < totalWrites; i++ {
        key := fmt.Sprintf("key%d", i)
        value := make([]byte, dataSize)
        for j := range value {
            value[j] = byte(i % 256) // Fill with some data
        }
        
        err := bt.Write(tableName, key, value)
        if err != nil {
            t.Fatalf("Failed to write data (iteration %d): %v", i, err)
        }
        
        if i % 100 == 0 {
            log.Printf("Written %d entries...", i)
        }
    }
    
    log.Println("Finished writing data")

    // Check if the files were actually created
    log.Println("Checking for created .gob files...")
    files, err := os.ReadDir(cwd)
    if err != nil {
        t.Fatalf("Failed to read current directory %s: %v", cwd, err)
    }

    flushedFiles := []string{}
    for _, file := range files {
        if strings.HasSuffix(file.Name(), ".gob") && strings.HasPrefix(file.Name(), tableName) {
            flushedFiles = append(flushedFiles, file.Name())
        }
    }

    if len(flushedFiles) == 0 {
        t.Fatalf("No .gob files were created for table %s in the current directory %s", tableName, cwd)
    }

    log.Printf("Found %d .gob files for table %s:", len(flushedFiles), tableName)
    for _, fileName := range flushedFiles {
        filePath := filepath.Join(cwd, fileName)
        fileInfo, err := os.Stat(filePath)
        if err != nil {
            log.Printf("- %s (unable to get file info: %v)", fileName, err)
        } else {
            log.Printf("- %s (size: %d bytes)", fileName, fileInfo.Size())
        }
    }

    // Clean up the flushed files after the test
    // defer func() {
    //     log.Println("Cleaning up .gob files...")
    //     for _, fileName := range flushedFiles {
    //         err := os.Remove(filepath.Join(cwd, fileName))
    //         if err != nil {
    //             log.Printf("Failed to remove file %s: %v", fileName, err)
    //         } else {
    //             log.Printf("Removed file: %s", fileName)
    //         }
    //     }
    // }()

    log.Println("Flush test completed successfully")
}