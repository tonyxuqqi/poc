package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

var columnsStr = "json_value"

type ColStore struct {
	session    *gocql.Session
	tableID    string
	binaryJSON bool
}

func NewColStore(session *gocql.Session, tableID string, binaryJSON bool) *ColStore {
	return &ColStore{
		session:    session,
		tableID:    tableID,
		binaryJSON: binaryJSON,
	}
}

func (s *ColStore) BatchInsert(keys []string, jsonValues []interface{}, indexKeysSlice []string) error {
	batch := s.session.NewBatch(gocql.LoggedBatch)
	for i, jsonValue := range jsonValues {
		var val interface{}
		if s.binaryJSON {
			jsonData, err := json.Marshal(jsonValue)
			if err != nil {
				return err
			}
			val = jsonData
		} else {
			val = jsonValue
		}
		batch.Query("INSERT INTO customer (id, json, name) VALUES (?, ?, ?)", keys[i], val, indexKeysSlice[i])
	}
	if err := s.session.ExecuteBatch(batch); err != nil {
		log.Fatalf("failed to execute batch: %v", err)
	}

	return nil
}

type KvStore struct {
	client *rawkv.Client
}

func NewKvStore(client *rawkv.Client) *KvStore {
	return &KvStore{
		client: client,
	}
}

func (s *KvStore) BatchInsert(keys []string, jsonValues []interface{}, indexKeysSlice []string) error {
	keysList := make([][]byte, len(keys))
	valuesList := make([][]byte, len(jsonValues))
	for i, jsonValue := range jsonValues {
		keysList[i] = []byte(fmt.Sprintf("t_%s", keys[i]))
		jsonData, err := json.Marshal(jsonValue)
		if err != nil {
			return err
		}
		valuesList[i] = jsonData

	}
	return s.client.BatchPut(context.TODO(), keysList, valuesList)
}

// SqlStore represents a SQL-based store
type SqlStore struct {
	conn       *sql.DB
	tableID    string
	binaryJSON bool
}

// NewSqlStore creates a new SqlStore instance
func NewSqlStore(conn *sql.DB, tableID string, binaryJSON bool) *SqlStore {
	return &SqlStore{
		conn:       conn,
		tableID:    tableID,
		binaryJSON: binaryJSON,
	}
}

// Insert inserts a JSON value into the store
func (s *SqlStore) Insert(key string, jsonValue interface{}, indexKeys []string) error {
	jsonData, err := json.Marshal(jsonValue)
	if err != nil {
		return err
	}

	tx, err := s.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var val interface{}
	if s.binaryJSON {
		val = jsonData
	} else {
		val = jsonValue
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.tableID, columnsStr, val)
	_, err = tx.Exec(sql, val)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// BatchInsert inserts multiple JSON values into the store
func (s *SqlStore) BatchInsert(keys []string, jsonValues []interface{}, indexKeysSlice []string) error {
	tx, err := s.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO customer (JSON) VALUES (?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, jsonValue := range jsonValues {
		jsonData, err := json.Marshal(jsonValue)
		if err != nil {
			return err
		}

		var val interface{}
		if s.binaryJSON {
			val = jsonData
		} else {
			val = jsonValue
		}

		_, err = stmt.Exec(val)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetBy retrieves a JSON value by index key
func (s *SqlStore) GetBy(indexKey string) ([]interface{}, error) {
	rows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", s.tableID, "name"), indexKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []interface{}
	for rows.Next() {
		var jsonValue interface{}
		if err := rows.Scan(&jsonValue); err != nil {
			return nil, err
		}
		result = append(result, jsonValue)
	}

	return result, nil
}

// MultiGetBy retrieves JSON values by multiple index keys
func (s *SqlStore) MultiGetBy(indexKeys []string) ([]interface{}, error) {
	args := make([]interface{}, len(indexKeys))
	for i, key := range indexKeys {
		args[i] = key
	}

	rows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s IN (?) LIMIT 100", s.tableID, "name"), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []interface{}
	for rows.Next() {
	}

	return result, nil
}

// Global variables to track throughput
var (
	totalKeysInserted int64
	totalKeysQueried  int64
	startTime         time.Time
	endTime           time.Time
	allTasksDone      chan struct{}
	payload           string
)

// Function to generate random data for insertion
func generateRandomData() string {
	return payload
}

// Function to generate random key
func generateRandomKey() string {
	return strings.Join(randStrings(10), "")
}

// Function to generate an array of random strings
func randStrings(n int) []string {
	var result []string
	for i := 0; i < n; i++ {
		result = append(result, randString())
	}
	return result
}

// Function to generate a random string
func randString() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 5)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randString2(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randString3(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Function to insert data into the database
func insertData(db *sql.DB, threadID int, wg *sync.WaitGroup) {
	sqlStore := NewSqlStore(db, "customer", true)
	for j := 0; j < 100000; j++ {
		// Generate random data
		jsons := make([]map[string]interface{}, 0)
		keys := make([]string, 0)
		names := make([]string, 0)
		for k := 0; k < 10; k++ {
			jsonContent := generateRandomData()
			key := generateRandomKey()
			name := generateRandomKey()
			jsons = append(jsons, map[string]interface{}{"id": key, "name": name, "json": jsonContent})
			keys = append(keys, key)
			names = append(names, name)
		}
		// Convert jsons to []interface{}
		jsonsInterface := make([]interface{}, len(jsons))
		for i, v := range jsons {
			jsonsInterface[i] = v
		}
		err := sqlStore.BatchInsert(keys, jsonsInterface, names)
		if err != nil {
			log.Println("Error inserting data:", err)
			break
		}
		// Increment the total keys inserted count
		atomic.AddInt64(&totalKeysInserted, 10)
	}
	wg.Done()
}

// Function to insert data into the database
func insertData2(db *ColStore, threadID int, wg *sync.WaitGroup) {
	payload = randString2(1024 * 32)
	for j := 0; j < 100000; j++ {
		// Generate random data
		jsons := make([]map[string]interface{}, 0)
		keys := make([]string, 0)
		names := make([]string, 0)
		for k := 0; k < 10; k++ {
			jsonContent := generateRandomData()
			key := generateRandomKey()
			name := generateRandomKey()
			jsons = append(jsons, map[string]interface{}{"id": key, "name": name, "json": jsonContent})
			keys = append(keys, key)
			names = append(names, name)
		}

		// Convert jsons to []interface{}
		jsonsInterface := make([]interface{}, len(jsons))
		for i, v := range jsons {
			jsonsInterface[i] = v
		}

		// Perform INSERT query
		err := db.BatchInsert(keys, jsonsInterface, names)
		if err != nil {
			log.Println("Error inserting data:", err)
			continue
		}

		// Increment the total keys inserted count
		atomic.AddInt64(&totalKeysInserted, 10)
	}
	fmt.Println("Finished inserting data")
	wg.Done()
}

// Function to insert data into the database
func insertData_Kv(kv *KvStore, threadID int, wg *sync.WaitGroup) {
	payload = randString2(1024 * 32)
	for j := 0; j < 100000; j++ {
		// Generate random data
		jsons := make([]map[string]interface{}, 0)
		keys := make([]string, 0)
		names := make([]string, 0)
		for k := 0; k < 50; k++ {
			jsonContent := generateRandomData()
			key := generateRandomKey()
			name := generateRandomKey()
			jsons = append(jsons, map[string]interface{}{"id": key, "name": name, "json": jsonContent})
			keys = append(keys, key)
			names = append(names, name)
		}

		// Convert jsons to []interface{}
		jsonsInterface := make([]interface{}, len(jsons))
		for i, v := range jsons {
			jsonsInterface[i] = v
		}

		// Perform INSERT query
		err := kv.BatchInsert(keys, jsonsInterface, names)
		if err != nil {
			log.Println("Error inserting data:", err)
			continue
		}

		// Increment the total keys inserted count
		atomic.AddInt64(&totalKeysInserted, 50)
	}
	fmt.Println("Finished inserting data")
	wg.Done()
}

func randomPick(allNames []string, count int) []string {
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	n := len(allNames)
	if count > n {
		count = n // Limit count to the length of the slice
	}
	selectedIndices := make(map[int]struct{})
	pickedNames := make([]string, 0, count)

	// Loop until count unique names are selected
	for len(pickedNames) < count {
		idx := rand.Intn(n)
		// Check if the index has not been selected before
		if _, ok := selectedIndices[idx]; !ok {
			selectedIndices[idx] = struct{}{}
			pickedNames = append(pickedNames, allNames[idx])
		}
	}

	return pickedNames
}

// Function to query data from the database
func queryData(allNames []string) {
	// Open a connection to the MySQL database
	db, err := sql.Open("mysql", "root@tcp(192.168.1.232:33721)/sbtest3")
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}
	defer db.Close()
	for i := 0; i < 100000; i++ {
		// Generate random name
		names := randomPick(allNames, 10)
		var quotedNames []string

		for _, name := range names {
			quotedNames = append(quotedNames, fmt.Sprintf("'%s'", name))
		}
		query := fmt.Sprintf("SELECT name FROM customer WHERE name IN (%s) limit 100", strings.Join(quotedNames, ", "))

		// Perform SELECT query
		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			log.Println("Error querying data:", err)
			continue
		}
		defer rows.Close()

		// Iterate over query results
		for rows.Next() {
			// Process row data
		}

		// Increment the total keys queried count
		atomic.AddInt64(&totalKeysQueried, 1)
	}
}

func getAllNames(db *sql.DB) []string {
	// Generate random name
	query := "SELECT distinct name FROM customer"

	// Perform SELECT query
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		log.Println("Error querying data:", err)
		return nil
	}
	defer rows.Close()

	// Iterate over query results
	names := make([]string, 0)
	for rows.Next() {
		// Process row data
		name := ""
		rows.Scan(&name)
		names = append(names, name)
	}
	return names
}

// Function to periodically measure throughput
func measureThroughput() {
	prevKeysInserted := totalKeysInserted
	prevKeysQueried := totalKeysQueried
	startTime = time.Now()

	for {
		time.Sleep(time.Second) // Measure throughput every second
		keysInsertedThisSecond := totalKeysInserted - prevKeysInserted
		keysQueriedThisSecond := totalKeysQueried - prevKeysQueried
		prevKeysInserted = totalKeysInserted
		prevKeysQueried = totalKeysQueried
		fmt.Printf("Throughput: Insert %d keys/second, Query %d keys/second\n", keysInsertedThisSecond, keysQueriedThisSecond)

		// Check if all tasks are done
		select {
		case <-allTasksDone:
			endTime = time.Now()
			return
		default:
		}
	}
}

func main() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())
	// Number of goroutines to spawn
	numGoroutines := 16

	// Start measuring throughput
	allTasksDone = make(chan struct{})
	go measureThroughput()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	db_type := flag.String("db", "tidb", "The database type, tidb or scylla")
	flag.Parse()
	if *db_type == "tidb" {
		fmt.Printf("Using TiDB\n")
		// Open a connection to the MySQL database
		db, err := sql.Open("mysql", "root@tcp(192.168.1.232:33721)/sbtest3")
		if err != nil {
			log.Fatal("Error connecting to database:", err)
		}
		defer db.Close()
		// Start goroutines for inserting data
		for i := 0; i < numGoroutines; i++ {
			go insertData(db, i, &wg)
		}

	} else if *db_type == "scylla" {
		fmt.Printf("Using ScyllaDB\n")
		cluster := gocql.NewCluster("127.0.0.1") // Replace with your ScyllaDB host
		cluster.Keyspace = "ns1"                 // Replace with your keyspace name
		cluster.Port = 19042
		cluster.Consistency = gocql.Quorum
		session, err := cluster.CreateSession()
		if err != nil {
			log.Fatal("Error connecting to database:", err)
			return
		}
		store := NewColStore(session, "customer", true)
		defer session.Close()
		// Start goroutines for inserting data
		for i := 0; i < numGoroutines; i++ {
			go insertData2(store, i, &wg)
		}

	} else if *db_type == "kv" {
		fmt.Printf("Using TiKV\n")
		cli, err := rawkv.NewClient(context.TODO(), []string{"40.76.113.99:2379", "172.210.66.23:2379", "20.84.67.12:2379"}, config.DefaultConfig().Security)
		if err != nil {
			log.Fatal("Error connecting to tikv:", err)
			return
		}
		kv := NewKvStore(cli)
		for i := 0; i < numGoroutines; i++ {
			go insertData_Kv(kv, i, &wg)
		}
	}

	// Wait for all goroutines to finish
	//allNames := getAllNames(db)
	/*for i := 0; i < numGoroutines; i++ {
		go func() {
			queryData(allNames)
			wg.Done()
		}()
	}*/
	wg.Wait()
	close(allTasksDone)

	// Calculate and print summary
	totalTime := endTime.Sub(startTime)
	avgThroughput := float64(totalKeysInserted) / totalTime.Seconds()
	fmt.Println("Total keys inserted:", totalKeysInserted)
	fmt.Println("Total keys queried:", totalKeysQueried)
	fmt.Println("Total time taken:", totalTime)
	fmt.Printf("Average keys inserted per second: %.2f\n", avgThroughput)
}
