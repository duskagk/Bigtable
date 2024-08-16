package bigtable

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestBigTableLargeDataSingleInsert(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	testTimeout := 5 * time.Minute
	timer := time.NewTimer(testTimeout)

	bt, err := NewBigTable("./temp")
	if err != nil {
		t.Fatalf("Failed to create BigTable instance: %v", err)
	}

	err = bt.CreateTable("users")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 데이터 삽입 (개별 삽입 사용)
	start := time.Now()
	for i := 0; i < 100000; i++ {
		select {
		case <-timer.C:
			t.Fatalf("Test timed out after %v while inserting data", testTimeout)
		default:
			userID := fmt.Sprintf("user%d", i)
			err := bt.Put("users", userID, "info", "name", []byte(fmt.Sprintf("User %d", i)))
			if err != nil {
				t.Fatalf("Failed to put name data: %v", err)
			}
			err = bt.Put("users", userID, "info", "email", []byte(fmt.Sprintf("user%d@example.com", i)))
			if err != nil {
				t.Fatalf("Failed to put email data: %v", err)
			}
			if i % 1000 == 0 && i > 0 {
				log.Printf("Inserted %d users", i)
			}
		}
	}
	insertDuration := time.Since(start)
	log.Printf("Total time taken to insert 10,000 users: %v", insertDuration)

	// 랜덤 데이터 검색
	start = time.Now()
	for i := 0; i < 1000; i++ {
		select {
		case <-timer.C:
			t.Fatalf("Test timed out after %v while performing random searches", testTimeout)
		default:
			userID := fmt.Sprintf("user%d", rand.Intn(10000))
			_, err := bt.Get("users", userID, "info", "name")
			if err != nil {
				t.Fatalf("Failed to get data: %v", err)
			}
			if i % 100 == 0 {
				log.Printf("Performed %d random searches", i)
			}
		}
	}
	searchDuration := time.Since(start)
	log.Printf("Time taken to perform 1,000 random searches: %v", searchDuration)

	// 순차적 데이터 검색
	start = time.Now()
	for i := 0; i < 1000; i++ {
		select {
		case <-timer.C:
			t.Fatalf("Test timed out after %v while performing sequential searches", testTimeout)
		default:
			userID := fmt.Sprintf("user%d", i)
			_, err := bt.Get("users", userID, "info", "name")
			if err != nil {
				t.Fatalf("Failed to get data: %v", err)
			}
			if i % 100 == 0 {
				log.Printf("Performed %d sequential searches", i)
			}
		}
	}
	seqSearchDuration := time.Since(start)
	log.Printf("Time taken to perform 1,000 sequential searches: %v", seqSearchDuration)
}

func TestDebugBigTableLoading(t *testing.T) {
	dataDir := "./temp"
	bt, err := NewBigTable(dataDir)
	if err != nil {
		t.Fatalf("Failed to create BigTable instance: %v", err)
	}

	// 1. 데이터 디렉토리 내용 출력
	fmt.Println("Data directory contents:")
	err = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("  %s\n", path)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk data directory: %v", err)
	}

	// 2. 로드된 테이블 정보 출력
	fmt.Println("\nLoaded tables:")
	for tableName, table := range bt.Tables {
		fmt.Printf("  Table: %s\n", tableName)
		fmt.Printf("    Number of SSTables: %d\n", len(table.ssTables))
		fmt.Printf("    Number of rows in memTable: %d\n", len(table.memTable.rows))
		
		// SSTable 정보 출력
		for i, sstable := range table.ssTables {
			fmt.Printf("    SSTable %d:\n", i)
			fmt.Printf("      File: %s\n", sstable.fileName)
			fmt.Printf("      Min Key: %s\n", sstable.minKey)
			fmt.Printf("      Max Key: %s\n", sstable.maxKey)
		}
	}

	// 3. 'users' 테이블이 존재하는지 확인
	if _, exists := bt.Tables["users"]; !exists {
		t.Errorf("'users' table does not exist in loaded tables")
	}

	// 4. loadFromDisk 메서드 실행 과정 로깅
	fmt.Println("\nRe-running loadFromDisk:")
	err = bt.loadFromDisk()
	if err != nil {
		t.Errorf("Failed to load from disk: %v", err)
	}
}

func TestVerifyExistingData(t *testing.T) {
	bt, err := NewBigTable("./temp")
	if err != nil {
		t.Fatalf("Failed to create BigTable instance: %v", err)
	}

	// Print table information
	for tableName, table := range bt.Tables {
		fmt.Printf("Table: %s\n", tableName)
		fmt.Printf("  Number of SSTables: %d\n", len(table.ssTables))
		fmt.Printf("  Number of rows in memTable: %d\n", len(table.memTable.rows))
	}

	// 테스트할 키 목록
	testKeys := []string{
		"user0", "user1", "user10", "user100", "user1000", "user10000",
		"user99999", // 마지막 키
	}

	for i := 0; i < 10; i++ {
		randomKey := fmt.Sprintf("user%d", rand.Intn(100000))
		testKeys = append(testKeys, randomKey)
	}

	for _, key := range testKeys {
		fmt.Printf("Attempting to get data for key: %s\n", key)
		name, err := bt.Get("users", key, "info", "name")
		if err != nil {
			t.Errorf("Failed to get name for key %s: %v", key, err)
			continue
		}
		email, err := bt.Get("users", key, "info", "email")
		if err != nil {
			t.Errorf("Failed to get email for key %s: %v", key, err)
			continue
		}

		expectedName := fmt.Sprintf("User %s", key[4:]) // "user" 접두사 제거
		expectedEmail := fmt.Sprintf("%s@example.com", key)

		fmt.Printf("Key: %s\n", key)
		fmt.Printf("  Name: %s (Expected: %s)\n", string(name), expectedName)
		fmt.Printf("  Email: %s (Expected: %s)\n", string(email), expectedEmail)
		fmt.Println("------------------------")

		if string(name) != expectedName {
			t.Errorf("Name mismatch for key %s. Got: %s, Expected: %s", key, string(name), expectedName)
		}
		if string(email) != expectedEmail {
			t.Errorf("Email mismatch for key %s. Got: %s, Expected: %s", key, string(email), expectedEmail)
		}
	}
}


func TestMemoryUsage(t *testing.T) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	beforeAlloc := m.Alloc

	bt, err := NewBigTable("./temp")
	if err != nil {
		t.Fatalf("Failed to create BigTable instance: %v", err)
	}

	runtime.ReadMemStats(&m)
	afterLoadAlloc := m.Alloc

	fmt.Printf("Memory used after loading: %v MB\n", (afterLoadAlloc - beforeAlloc) / 1024 / 1024)

	// Perform some random gets
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user%d", rand.Intn(100000))
		_, err := bt.Get("users", key, "info", "name")
		if err != nil {
			t.Errorf("Failed to get data for key %s: %v", key, err)
		}
	}

	runtime.ReadMemStats(&m)
	afterGetsAlloc := m.Alloc

	fmt.Printf("Total memory in use: %v MB\n", afterGetsAlloc / 1024 / 1024)
	fmt.Printf("Additional memory used after gets: %v MB\n", (afterGetsAlloc - afterLoadAlloc) / 1024 / 1024)
}


func TestBigTableLargeDataWithExtendedVariableInsertPattern(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	testTimeout := 10 * time.Minute
	timer := time.NewTimer(testTimeout)

	bt, err := NewBigTable("./temp")
	if err != nil {
		t.Fatalf("Failed to create BigTable instance: %v", err)
	}

	err = bt.CreateTable("users")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	start := time.Now()

	// 다섯 가지 패턴으로 데이터 삽입
	patterns := []int{0, 1, 2, 3, 4}
	for _, startValue := range patterns {
		for i := startValue; i < 20000; i += 5 {
			select {
			case <-timer.C:
				t.Fatalf("Test timed out after %v while inserting data", testTimeout)
			default:
				userID := fmt.Sprintf("user%d", i)
				err := bt.Put("users", userID, "info", "name", []byte(fmt.Sprintf("User %d", i)))
				if err != nil {
					t.Fatalf("Failed to put name data: %v", err)
				}
				err = bt.Put("users", userID, "info", "email", []byte(fmt.Sprintf("user%d@example.com", i)))
				if err != nil {
					t.Fatalf("Failed to put email data: %v", err)
				}
				if i % 1000 == startValue && i > startValue {
					log.Printf("Inserted %d users in pattern starting with %d", i, startValue)
				}
			}
		}
	}

	insertDuration := time.Since(start)
	log.Printf("Total time taken to insert users with extended variable pattern: %v", insertDuration)

	// 랜덤 데이터 검색
	start = time.Now()
	for i := 0; i < 1000; i++ {
		select {
		case <-timer.C:
			t.Fatalf("Test timed out after %v while performing random searches", testTimeout)
		default:
			userID := fmt.Sprintf("user%d", rand.Intn(20000))
			_, err := bt.Get("users", userID, "info", "name")
			if err != nil {
				t.Fatalf("Failed to get data for key %s: %v", userID, err)
			}
			if i % 100 == 0 {
				log.Printf("Performed %d random searches", i)
			}
		}
	}
	searchDuration := time.Since(start)
	log.Printf("Time taken to perform 1,000 random searches: %v", searchDuration)

	// 순차적 데이터 검색 (각 패턴에 대해)
	start = time.Now()
	for _, startValue := range patterns {
		for i := 0; i < 200; i++ {
			select {
			case <-timer.C:
				t.Fatalf("Test timed out after %v while performing sequential searches", testTimeout)
			default:
				userID := fmt.Sprintf("user%d", startValue + i*5)
				_, err := bt.Get("users", userID, "info", "name")
				if err != nil {
					t.Fatalf("Failed to get data for key %s: %v", userID, err)
				}
				if i % 20 == 0 {
					log.Printf("Performed %d sequential searches in pattern starting with %d", i, startValue)
				}
			}
		}
	}
	seqSearchDuration := time.Since(start)
	log.Printf("Time taken to perform 1,000 sequential searches (200 for each pattern): %v", seqSearchDuration)

	// 경계값 검색
	start = time.Now()
	boundaryKeys := []string{"user0", "user19995", "user19996", "user19997", "user19998", "user19999"}
	for _, key := range boundaryKeys {
		_, err := bt.Get("users", key, "info", "name")
		if err != nil {
			t.Fatalf("Failed to get data for boundary key %s: %v", key, err)
		}
	}
	boundarySearchDuration := time.Since(start)
	log.Printf("Time taken to perform boundary value searches: %v", boundarySearchDuration)
}