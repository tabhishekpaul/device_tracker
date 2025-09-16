package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	_ "github.com/mattn/go-sqlite3"
)

type LoaderConfig struct {
	CHHost        string
	CHPort        int
	CHDatabase    string
	CHUser        string
	CHPassword    string
	MaxWorkers    int
	ChunkSize     int
	LocalDBPath   string
	RetryAttempts int
	QueryTimeout  time.Duration
	// New performance settings
	MaxConnections     int
	BatchInsertWorkers int
	FileReadBuffer     int
	MemoryLimit        int64 // bytes
	MaxBatchSize       int
}

type LoaderStats struct {
	TotalFilesProcessed int64
	TotalRowsInserted   int64
	FailedFiles         []string
	SkippedFiles        []string
	StartTime           time.Time
	EndTime             time.Time
	mu                  sync.RWMutex
}

func (s *LoaderStats) IncrementProcessed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalFilesProcessed++
}

func (s *LoaderStats) AddRows(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalRowsInserted += count
}

func (s *LoaderStats) AddFailedFile(filePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FailedFiles = append(s.FailedFiles, filePath)
}

func (s *LoaderStats) AddSkippedFile(filePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SkippedFiles = append(s.SkippedFiles, filePath)
}

func (s *LoaderStats) GetStats() (int64, int64, []string, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalFilesProcessed, s.TotalRowsInserted, s.FailedFiles, s.SkippedFiles
}

type ProcessedFile struct {
	FilePath    string
	FileHash    string
	ProcessedAt time.Time
	RecordCount int64
	LoadDate    string
}

type FileProcessResult struct {
	FilePath       string
	RowsProcessed  int64
	Success        bool
	Error          error
	ProcessingTime time.Duration
	StartTime      time.Time
	WorkerID       int
	Retried        bool
}

type DataBatch struct {
	DeviceIDs  []string
	Timestamps []time.Time
	Latitudes  []float64
	Longitudes []float64
	LoadDate   time.Time
	Size       int
}

type ParquetToClickHouseLoader struct {
	config    LoaderConfig
	stats     *LoaderStats
	localDB   *sql.DB
	connPool  chan *sql.DB
	batchChan chan *DataBatch
	batchWg   sync.WaitGroup
}

func NewParquetToClickHouseLoader(config LoaderConfig) (*ParquetToClickHouseLoader, error) {
	// Set GOMAXPROCS to use all available CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize local SQLite database with performance settings
	localDB, err := sql.Open("sqlite3", config.LocalDBPath+"?cache=shared&mode=rwc&_journal_mode=WAL&_synchronous=NORMAL&_cache_size=100000")
	if err != nil {
		return nil, fmt.Errorf("failed to open local database: %w", err)
	}

	// Configure SQLite for performance
	localDB.SetMaxOpenConns(20)
	localDB.SetMaxIdleConns(10)
	localDB.SetConnMaxLifetime(time.Hour)

	loader := &ParquetToClickHouseLoader{
		config:    config,
		localDB:   localDB,
		connPool:  make(chan *sql.DB, config.MaxConnections),
		batchChan: make(chan *DataBatch, config.MaxConnections*2),
		stats: &LoaderStats{
			StartTime: time.Now(),
		},
	}

	if err := loader.initLocalDB(); err != nil {
		return nil, fmt.Errorf("failed to initialize local database: %w", err)
	}

	// Initialize connection pool
	if err := loader.initConnectionPool(); err != nil {
		return nil, fmt.Errorf("failed to initialize connection pool: %w", err)
	}

	// Start batch insert workers
	loader.startBatchInsertWorkers()

	return loader, nil
}

func (loader *ParquetToClickHouseLoader) initConnectionPool() error {
	log.Printf("Initializing connection pool with %d connections...", loader.config.MaxConnections)

	for i := 0; i < loader.config.MaxConnections; i++ {
		conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
		if err != nil {
			return fmt.Errorf("failed to create connection %d: %w", i, err)
		}

		// Configure connection for high performance
		conn.SetMaxOpenConns(1)
		conn.SetMaxIdleConns(1)
		conn.SetConnMaxLifetime(0) // No expiration

		// Test connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := conn.PingContext(ctx); err != nil {
			cancel()
			return fmt.Errorf("failed to ping connection %d: %w", i, err)
		}
		cancel()

		loader.connPool <- conn
	}

	log.Printf("Connection pool initialized successfully")
	return nil
}

func (loader *ParquetToClickHouseLoader) getConnection() *sql.DB {
	return <-loader.connPool
}

func (loader *ParquetToClickHouseLoader) returnConnection(conn *sql.DB) {
	loader.connPool <- conn
}

func (loader *ParquetToClickHouseLoader) startBatchInsertWorkers() {
	log.Printf("Starting %d batch insert workers...", loader.config.BatchInsertWorkers)

	for i := 0; i < loader.config.BatchInsertWorkers; i++ {
		loader.batchWg.Add(1)
		go func(workerID int) {
			defer loader.batchWg.Done()
			loader.batchInsertWorker(workerID)
		}(i)
	}
}

func (loader *ParquetToClickHouseLoader) batchInsertWorker(workerID int) {
	for batch := range loader.batchChan {
		start := time.Now()

		conn := loader.getConnection()
		err := loader.insertBatch(conn, batch)
		loader.returnConnection(conn)

		if err != nil {
			log.Printf("Batch insert worker %d failed: %v", workerID, err)
			// You might want to implement retry logic here
		} else {
			duration := time.Since(start)
			rowsPerSec := float64(batch.Size) / duration.Seconds()
			log.Printf("Worker %d: Inserted %d rows in %.2fs (%.0f rows/sec)",
				workerID, batch.Size, duration.Seconds(), rowsPerSec)
		}
	}
}

func (loader *ParquetToClickHouseLoader) insertBatch(conn *sql.DB, batch *DataBatch) error {
	if batch.Size == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Prepare the query with placeholders
	placeholders := make([]string, batch.Size)
	args := make([]interface{}, 0, batch.Size*5)

	for i := 0; i < batch.Size; i++ {
		placeholders[i] = "(?, ?, ?, ?, ?)"
		args = append(args,
			batch.DeviceIDs[i],
			batch.Timestamps[i],
			batch.Latitudes[i],
			batch.Longitudes[i],
			batch.LoadDate)
	}

	query := fmt.Sprintf(`
		INSERT INTO device_data 
		(device_id, event_timestamp, latitude, longitude, load_date) 
		VALUES %s`, strings.Join(placeholders, ","))

	_, err := conn.ExecContext(ctx, query, args...)
	return err
}

func (loader *ParquetToClickHouseLoader) initLocalDB() error {
	createTableSQL := `
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = 100000;
	PRAGMA temp_store = MEMORY;
	PRAGMA mmap_size = 268435456;
	
	CREATE TABLE IF NOT EXISTS processed_files (
		id INTEGER PRIMARY KEY,
		file_path TEXT NOT NULL UNIQUE,
		file_hash TEXT NOT NULL,
		processed_at DATETIME NOT NULL,
		record_count INTEGER NOT NULL,
		load_date TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path);
	CREATE INDEX IF NOT EXISTS idx_file_hash ON processed_files(file_hash);
	CREATE INDEX IF NOT EXISTS idx_load_date ON processed_files(load_date);
	`

	if _, err := loader.localDB.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	log.Printf("Local tracking database initialized at: %s", loader.config.LocalDBPath)
	return nil
}

func (loader *ParquetToClickHouseLoader) calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	// Use larger buffer for faster hashing
	buffer := make([]byte, 1024*1024) // 1MB buffer

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			hash.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (loader *ParquetToClickHouseLoader) isFileProcessed(filePath string) (bool, string, error) {
	// Calculate current file hash
	currentHash, err := loader.calculateFileHash(filePath)
	if err != nil {
		return false, "", fmt.Errorf("failed to calculate file hash: %w", err)
	}

	// Check if file with same path and hash exists
	var existingHash string
	var processedAt string
	query := "SELECT file_hash, processed_at FROM processed_files WHERE file_path = ?"

	err = loader.localDB.QueryRow(query, filePath).Scan(&existingHash, &processedAt)
	if err == sql.ErrNoRows {
		return false, currentHash, nil // File not processed
	}
	if err != nil {
		return false, "", fmt.Errorf("failed to check processed files: %w", err)
	}

	// If hashes match, file was already processed
	if existingHash == currentHash {
		log.Printf("File %s already processed at %s (hash: %s)",
			filepath.Base(filePath), processedAt, currentHash[:8])
		return true, currentHash, nil
	}

	// File exists but hash is different (file was modified)
	log.Printf("File %s was modified since last processing, will reprocess", filepath.Base(filePath))
	return false, currentHash, nil
}

func (loader *ParquetToClickHouseLoader) markFileAsProcessed(filePath, fileHash string, recordCount int64, loadDate string) error {
	// Use REPLACE to handle both insert and update cases
	query := `
	REPLACE INTO processed_files (file_path, file_hash, processed_at, record_count, load_date)
	VALUES (?, ?, ?, ?, ?)
	`

	_, err := loader.localDB.Exec(query, filePath, fileHash, time.Now(), recordCount, loadDate)
	if err != nil {
		return fmt.Errorf("failed to mark file as processed: %w", err)
	}

	return nil
}

func (loader *ParquetToClickHouseLoader) getProcessedFilesStats() (int, int64, error) {
	var totalFiles int
	var totalRecords int64

	query := "SELECT COUNT(*), COALESCE(SUM(record_count), 0) FROM processed_files"
	err := loader.localDB.QueryRow(query).Scan(&totalFiles, &totalRecords)

	return totalFiles, totalRecords, err
}

func (loader *ParquetToClickHouseLoader) getClickHouseURL() string {
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&max_execution_time=0&max_memory_usage=0&max_block_size=1048576&max_insert_block_size=1048576",
		loader.config.CHHost, loader.config.CHPort,
		loader.config.CHUser, loader.config.CHPassword, loader.config.CHDatabase)
}

func (loader *ParquetToClickHouseLoader) setupDatabase() error {
	log.Println("Setting up ClickHouse database and tables...")

	// Debug: print the connection string (without password)
	connStr := loader.getClickHouseURL()
	debugConnStr := strings.Replace(connStr, loader.config.CHPassword, "***", 1)
	log.Printf("Using connection string: %s", debugConnStr)

	conn := loader.getConnection()
	defer loader.returnConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Test connection first
	var testResult int
	err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
	if err != nil {
		return fmt.Errorf("failed to test connection: %w", err)
	}
	log.Printf("Connection test successful: %d", testResult)

	// Create database if not exists
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", loader.config.CHDatabase))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Check if table exists and has data
	var tableExists bool
	err = conn.QueryRowContext(ctx, "EXISTS TABLE device_data").Scan(&tableExists)
	if err == nil && tableExists {
		var rowCount int64
		err = conn.QueryRowContext(ctx, "SELECT count() FROM device_data").Scan(&rowCount)
		if err == nil && rowCount > 0 {
			log.Printf("Table 'device_data' exists with %s rows. Continuing from where we left off...", formatNumber(rowCount))
			return nil
		}
	}

	// Drop and recreate table only if starting fresh
	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS device_data")
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Optimized table schema for high-performance inserts
	createTableSQL := `
        CREATE TABLE device_data (
                device_id LowCardinality(String),
                event_timestamp DateTime64(3),
                latitude Float64 CODEC(DoubleDelta, ZSTD(1)),
                longitude Float64 CODEC(DoubleDelta, ZSTD(1)),
                load_date Date
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(load_date)
        ORDER BY (load_date, cityHash64(device_id), event_timestamp)
        SETTINGS 
                index_granularity = 8192,
                compress_marks = 1,
                compress_primary_key = 1,
                max_compress_block_size = 8388608,
                min_compress_block_size = 262144,
                max_insert_block_size = 16777216,
                max_threads = 64,
                max_insert_threads = 32,
                max_memory_usage = 0,
                optimize_on_insert = 0,
                async_insert = 1,
                wait_for_async_insert = 0
        `

	_, err = conn.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("ClickHouse table 'device_data' created successfully with optimized settings")
	return nil
}

func (loader *ParquetToClickHouseLoader) getParquetFiles(baseFolders []string) ([]struct {
	path     string
	loadDate time.Time
}, error) {
	var allFiles []struct {
		path     string
		loadDate time.Time
	}

	// Process folders in parallel
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, folder := range baseFolders {
		wg.Add(1)
		go func(folder string) {
			defer wg.Done()

			if _, err := os.Stat(folder); os.IsNotExist(err) {
				log.Printf("Warning: Folder does not exist: %s", folder)
				return
			}

			folderName := filepath.Base(folder)
			if !strings.Contains(folderName, "load_date=") {
				log.Printf("Error: Folder name doesn't contain load_date: %s", folder)
				return
			}

			dateStr := strings.Split(folderName, "=")[1]
			loadDate, err := time.Parse("20060102", dateStr)
			if err != nil {
				log.Printf("Error: Invalid date format in folder: %s", folder)
				return
			}

			pattern := filepath.Join(folder, "*.parquet")
			files, err := filepath.Glob(pattern)
			if err != nil {
				log.Printf("Error scanning folder %s: %v", folder, err)
				return
			}

			mu.Lock()
			for _, filePath := range files {
				allFiles = append(allFiles, struct {
					path     string
					loadDate time.Time
				}{filePath, loadDate})
			}
			mu.Unlock()

			log.Printf("Found %d parquet files in %s", len(files), folder)
		}(folder)
	}

	wg.Wait()

	log.Printf("Total parquet files found: %d", len(allFiles))
	return allFiles, nil
}

func (loader *ParquetToClickHouseLoader) processSingleFile(filePath string, loadDate time.Time) FileProcessResult {
	result := FileProcessResult{
		FilePath:  filePath,
		StartTime: time.Now(),
	}

	log.Printf("Processing file: %s", filepath.Base(filePath))

	// Check if file was already processed
	isProcessed, fileHash, err := loader.isFileProcessed(filePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to check if file was processed: %w", err)
		return result
	}

	if isProcessed {
		log.Printf("Skipping already processed file: %s", filepath.Base(filePath))
		loader.stats.AddSkippedFile(filePath)
		result.Success = true
		return result
	}

	// Open parquet file with larger buffer
	osFile, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %w", err)
		return result
	}
	defer osFile.Close()

	// Create parquet reader
	pf, err := file.NewParquetReader(osFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to create parquet reader: %w", err)
		return result
	}
	defer pf.Close()

	// Create Arrow reader with optimized settings
	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		BatchSize: int64(loader.config.MaxBatchSize),
		Parallel:  true,
	}, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create arrow reader: %w", err)
		return result
	}

	// Read specific columns
	schema, err := arrowReader.Schema()
	if err != nil {
		result.Error = fmt.Errorf("failed to get schema: %w", err)
		return result
	}

	requiredColumns := []string{"device_id", "event_timestamp", "latitude", "longitude"}
	columnIndices := []int{}

	for i, field := range schema.Fields() {
		for _, reqCol := range requiredColumns {
			if strings.EqualFold(field.Name, reqCol) {
				columnIndices = append(columnIndices, i)
				break
			}
		}
	}

	if len(columnIndices) != len(requiredColumns) {
		result.Error = fmt.Errorf("not all required columns found in parquet file")
		return result
	}

	// Read the table
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		result.Error = fmt.Errorf("failed to read table: %w", err)
		return result
	}
	defer table.Release()

	numRows := int(table.NumRows())
	if numRows == 0 {
		log.Printf("Warning: Empty file: %s", filePath)

		// Mark empty file as processed
		if err := loader.markFileAsProcessed(filePath, fileHash, 0, loadDate.Format("20060102")); err != nil {
			log.Printf("Warning: failed to mark empty file as processed: %v", err)
		}

		result.Success = true
		return result
	}

	// Process with retry logic
	var rowsInserted int64
	for attempt := 0; attempt <= loader.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("Retry attempt %d for file: %s", attempt, filepath.Base(filePath))
			result.Retried = true
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		rowsInserted, err = loader.insertTableData(table, loadDate)
		if err == nil {
			break
		}

		if attempt == loader.config.RetryAttempts {
			result.Error = fmt.Errorf("failed to insert data after %d attempts: %w", attempt+1, err)
			return result
		}
	}

	// Mark file as processed
	if err := loader.markFileAsProcessed(filePath, fileHash, rowsInserted, loadDate.Format("20060102")); err != nil {
		log.Printf("Warning: failed to mark file as processed: %v", err)
	}

	result.RowsProcessed = rowsInserted
	result.Success = true
	result.ProcessingTime = time.Since(result.StartTime)

	log.Printf("Successfully processed %s: %d rows in %v",
		filepath.Base(filePath), rowsInserted, result.ProcessingTime)

	return result
}

func (loader *ParquetToClickHouseLoader) insertTableData(table arrow.Table, loadDate time.Time) (int64, error) {
	// Find column indices
	schema := table.Schema()
	deviceIDIdx, timestampIdx, latIdx, lonIdx := -1, -1, -1, -1

	for i, field := range schema.Fields() {
		switch strings.ToLower(field.Name) {
		case "device_id":
			deviceIDIdx = i
		case "event_timestamp":
			timestampIdx = i
		case "latitude":
			latIdx = i
		case "longitude":
			lonIdx = i
		}
	}

	if deviceIDIdx == -1 || timestampIdx == -1 || latIdx == -1 || lonIdx == -1 {
		return 0, fmt.Errorf("required columns not found")
	}

	// Get columns
	deviceIDCol := table.Column(deviceIDIdx)
	timestampCol := table.Column(timestampIdx)
	latCol := table.Column(latIdx)
	lonCol := table.Column(lonIdx)

	// Process data in large batches for maximum throughput
	numRows := int(table.NumRows())
	var totalInserted int64

	batchSize := loader.config.ChunkSize

	for start := 0; start < numRows; start += batchSize {
		end := start + batchSize
		if end > numRows {
			end = numRows
		}

		batch := &DataBatch{
			DeviceIDs:  make([]string, 0, end-start),
			Timestamps: make([]time.Time, 0, end-start),
			Latitudes:  make([]float64, 0, end-start),
			Longitudes: make([]float64, 0, end-start),
			LoadDate:   loadDate,
		}

		for i := start; i < end; i++ {
			deviceID := loader.getStringValue(deviceIDCol, i)
			timestamp := loader.getTimestampValue(timestampCol, i)
			latitude := loader.getFloat64Value(latCol, i)
			longitude := loader.getFloat64Value(lonCol, i)

			// Validate data
			if deviceID == "" || latitude < -90 || latitude > 90 ||
				longitude < -180 || longitude > 180 || timestamp.IsZero() {
				continue
			}

			batch.DeviceIDs = append(batch.DeviceIDs, deviceID)
			batch.Timestamps = append(batch.Timestamps, timestamp)
			batch.Latitudes = append(batch.Latitudes, latitude)
			batch.Longitudes = append(batch.Longitudes, longitude)
		}

		batch.Size = len(batch.DeviceIDs)

		if batch.Size > 0 {
			// Send batch to worker pool
			loader.batchChan <- batch
			totalInserted += int64(batch.Size)
		}

		if numRows > batchSize && start%100000 == 0 {
			log.Printf("Queued %d/%d rows for insertion", start+batch.Size, numRows)
		}
	}

	return totalInserted, nil
}

// Helper functions for extracting values from Arrow columns
func (loader *ParquetToClickHouseLoader) getStringValue(col *arrow.Column, row int) string {
	if col.Len() <= row {
		return ""
	}

	chunk := col.Data().Chunk(0)
	if chunk.IsNull(row) {
		return ""
	}

	switch arr := chunk.(type) {
	case *array.String:
		return arr.Value(row)
	case *array.Binary:
		return string(arr.Value(row))
	default:
		return fmt.Sprintf("%v", arr)
	}
}

func (loader *ParquetToClickHouseLoader) getTimestampValue(col *arrow.Column, row int) time.Time {
	if col.Len() <= row {
		return time.Time{}
	}

	chunk := col.Data().Chunk(0)
	if chunk.IsNull(row) {
		return time.Time{}
	}

	switch arr := chunk.(type) {
	case *array.Timestamp:
		return arr.Value(row).ToTime(arrow.Nanosecond)
	default:
		return time.Time{}
	}
}

func (loader *ParquetToClickHouseLoader) getFloat64Value(col *arrow.Column, row int) float64 {
	if col.Len() <= row {
		return 0.0
	}

	chunk := col.Data().Chunk(0)
	if chunk.IsNull(row) {
		return 0.0
	}

	switch arr := chunk.(type) {
	case *array.Float64:
		return arr.Value(row)
	case *array.Float32:
		return float64(arr.Value(row))
	default:
		return 0.0
	}
}

func (loader *ParquetToClickHouseLoader) loadParquetFilesParallel(parquetFolders []string) error {
	loader.stats.StartTime = time.Now()
	log.Printf("Starting parallel data loading with %d file workers and %d batch workers...",
		loader.config.MaxWorkers, loader.config.BatchInsertWorkers)

	// Get initial stats from tracking database
	processedFiles, processedRecords, _ := loader.getProcessedFilesStats()
	log.Printf("Previously processed: %d files, %s records", processedFiles, formatNumber(processedRecords))

	allFiles, err := loader.getParquetFiles(parquetFolders)
	if err != nil {
		return fmt.Errorf("failed to get parquet files: %w", err)
	}

	if len(allFiles) == 0 {
		log.Println("No parquet files found to process!")
		return nil
	}

	// Filter files that need processing in parallel
	var filesToProcess []struct {
		path     string
		loadDate time.Time
	}

	type checkResult struct {
		file struct {
			path     string
			loadDate time.Time
		}
		needsProcessing bool
	}

	checkChan := make(chan struct {
		path     string
		loadDate time.Time
	}, len(allFiles))

	resultChan := make(chan checkResult, len(allFiles))

	// Start workers to check file processing status
	checkWorkers := runtime.NumCPU()
	var checkWg sync.WaitGroup

	for i := 0; i < checkWorkers; i++ {
		checkWg.Add(1)
		go func() {
			defer checkWg.Done()
			for file := range checkChan {
				isProcessed, _, err := loader.isFileProcessed(file.path)
				needsProcessing := err == nil && !isProcessed
				resultChan <- checkResult{file: file, needsProcessing: needsProcessing}
			}
		}()
	}

	// Send files to check
	go func() {
		for _, file := range allFiles {
			checkChan <- file
		}
		close(checkChan)
	}()

	// Close result channel when all checks are done
	go func() {
		checkWg.Wait()
		close(resultChan)
	}()

	// Collect results
	skippedCount := 0
	for result := range resultChan {
		if result.needsProcessing {
			filesToProcess = append(filesToProcess, result.file)
		} else {
			skippedCount++
		}
	}

	log.Printf("Total files found: %d", len(allFiles))
	log.Printf("Files to process: %d", len(filesToProcess))
	log.Printf("Files already processed: %d", skippedCount)

	if len(filesToProcess) == 0 {
		log.Println("All files have already been processed!")
		return nil
	}

	filesChan := make(chan struct {
		path     string
		loadDate time.Time
	}, len(filesToProcess))
	resultsChan := make(chan FileProcessResult, len(filesToProcess))

	var wg sync.WaitGroup
	for i := 0; i < loader.config.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for file := range filesChan {
				result := loader.processSingleFile(file.path, file.loadDate)
				result.WorkerID = workerID
				resultsChan <- result
			}
		}(i)
	}

	go func() {
		for _, file := range filesToProcess {
			filesChan <- file
		}
		close(filesChan)
	}()

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	completed := 0
	for result := range resultsChan {
		completed++

		if result.Success {
			loader.stats.IncrementProcessed()
			loader.stats.AddRows(result.RowsProcessed)
			retryStatus := ""
			if result.Retried {
				retryStatus = " (retried)"
			}
			log.Printf("✅ (%d/%d) Worker %d Completed: %s - %d rows in %.2fs%s",
				completed, len(filesToProcess), result.WorkerID, filepath.Base(result.FilePath),
				result.RowsProcessed, result.ProcessingTime.Seconds(), retryStatus)
		} else {
			loader.stats.AddFailedFile(result.FilePath)
			log.Printf("❌ (%d/%d) Worker %d Failed: %s - %v",
				completed, len(filesToProcess), result.WorkerID, filepath.Base(result.FilePath), result.Error)
		}
	}

	// Close batch channel and wait for all batches to be processed
	close(loader.batchChan)
	loader.batchWg.Wait()

	loader.stats.EndTime = time.Now()
	loader.printFinalStats()
	return nil
}

func (loader *ParquetToClickHouseLoader) printFinalStats() {
	duration := loader.stats.EndTime.Sub(loader.stats.StartTime)
	processed, totalRows, failedFiles, skippedFiles := loader.stats.GetStats()

	log.Println("=" + strings.Repeat("=", 100))
	log.Println("HIGH-PERFORMANCE DATA LOADING COMPLETED")
	log.Println("=" + strings.Repeat("=", 100))
	log.Printf("Total processing time: %v", duration)
	log.Printf("Files processed successfully: %d", processed)
	log.Printf("Files skipped (already processed): %d", len(skippedFiles))
	log.Printf("Files failed: %d", len(failedFiles))
	log.Printf("New rows inserted this run: %s", formatNumber(totalRows))

	if totalRows > 0 && duration.Seconds() > 0 {
		rowsPerSecond := float64(totalRows) / duration.Seconds()
		log.Printf("Average insertion rate: %s rows/second", formatNumber(int64(rowsPerSecond)))

		// Calculate throughput per worker
		avgPerWorker := rowsPerSecond / float64(loader.config.MaxWorkers)
		log.Printf("Average per file worker: %.0f rows/second", avgPerWorker)

		avgPerBatchWorker := rowsPerSecond / float64(loader.config.BatchInsertWorkers)
		log.Printf("Average per batch worker: %.0f rows/second", avgPerBatchWorker)
	}

	// Get total stats from tracking database
	totalFiles, totalRecords, _ := loader.getProcessedFilesStats()
	log.Printf("Total files in tracking DB: %d", totalFiles)
	log.Printf("Total records in tracking DB: %s", formatNumber(totalRecords))

	// Memory usage stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Memory usage - Alloc: %s MB, Sys: %s MB",
		formatNumber(int64(m.Alloc/1024/1024)),
		formatNumber(int64(m.Sys/1024/1024)))

	if len(failedFiles) > 0 {
		log.Println("\nFailed files:")
		for _, failedFile := range failedFiles {
			log.Printf("  - %s", filepath.Base(failedFile))
		}
	}

	loader.verifyData()
}

func (loader *ParquetToClickHouseLoader) verifyData() {
	log.Println("\nPerforming data verification...")

	conn := loader.getConnection()
	defer loader.returnConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var totalCount, uniqueDevices int64
	var minDate, maxDate string

	if err := conn.QueryRowContext(ctx, "SELECT count() FROM device_data").Scan(&totalCount); err != nil {
		log.Printf("Failed to get total count: %v", err)
		return
	}

	if err := conn.QueryRowContext(ctx, "SELECT uniq(device_id) FROM device_data").Scan(&uniqueDevices); err != nil {
		log.Printf("Failed to get unique devices: %v", err)
		return
	}

	if err := conn.QueryRowContext(ctx, "SELECT min(load_date), max(load_date) FROM device_data").Scan(&minDate, &maxDate); err != nil {
		log.Printf("Failed to get date range: %v", err)
		return
	}

	// Additional performance metrics
	var avgRowsPerPartition float64
	if err := conn.QueryRowContext(ctx, "SELECT avg(cnt) FROM (SELECT count() as cnt FROM device_data GROUP BY toYYYYMM(load_date))").Scan(&avgRowsPerPartition); err == nil {
		log.Printf("Average rows per partition: %s", formatNumber(int64(avgRowsPerPartition)))
	}

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("CLICKHOUSE DATA VERIFICATION RESULTS")
	log.Println(strings.Repeat("=", 60))
	log.Printf("Total rows in database: %s", formatNumber(totalCount))
	log.Printf("Unique devices: %s", formatNumber(uniqueDevices))
	log.Printf("Date range: %s to %s", minDate, maxDate)
}

func (loader *ParquetToClickHouseLoader) optimizeTable() error {
	log.Println("Optimizing ClickHouse table for maximum performance...")

	conn := loader.getConnection()
	defer loader.returnConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Run multiple optimization operations
	optimizations := []struct {
		name  string
		query string
	}{
		{"OPTIMIZE TABLE", "OPTIMIZE TABLE device_data FINAL"},
		{"Update statistics", "ANALYZE TABLE device_data"},
	}

	for _, opt := range optimizations {
		log.Printf("Running: %s", opt.name)
		start := time.Now()

		_, err := conn.ExecContext(ctx, opt.query)
		if err != nil {
			log.Printf("Warning: %s failed: %v", opt.name, err)
			continue
		}

		log.Printf("%s completed in %v", opt.name, time.Since(start))
	}

	log.Println("Table optimization completed")
	return nil
}

func (loader *ParquetToClickHouseLoader) Close() {
	log.Println("Shutting down loader...")

	// Close batch channel if not already closed
	select {
	case <-loader.batchChan:
	default:
		close(loader.batchChan)
	}

	// Wait for all batch workers to complete
	loader.batchWg.Wait()

	// Close all connections in the pool
	close(loader.connPool)
	for conn := range loader.connPool {
		conn.Close()
	}

	if loader.localDB != nil {
		loader.localDB.Close()
	}

	log.Println("Loader shutdown complete")
}

func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	result := ""
	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(char)
	}
	return result
}

func main() {
	// Set garbage collection target to 50% to reduce GC pressure with high memory usage
	runtime.GC()

	// Maximum performance configuration for 64 vCPU / 128GB RAM server
	config := LoaderConfig{
		CHHost:     "localhost",
		CHPort:     9000,
		CHDatabase: "device_tracking",
		CHUser:     "default",
		CHPassword: "nyros",

		// File processing workers - use most CPUs for file processing
		MaxWorkers: 56, // Leave 8 CPUs for system and batch workers

		// Batch insert workers - dedicated workers for database inserts
		BatchInsertWorkers: 8,

		// Connection pool - one connection per batch worker + extras
		MaxConnections: 16,

		// Large chunk sizes for maximum throughput
		ChunkSize:    2000000, // 2M rows per chunk
		MaxBatchSize: 1000000, // 1M rows per batch for Arrow reader

		// Memory settings (use ~100GB of 128GB available)
		MemoryLimit: 100 * 1024 * 1024 * 1024, // 100GB

		// File and performance settings
		FileReadBuffer: 16 * 1024 * 1024, // 16MB read buffer
		LocalDBPath:    "./parquet_tracking.db",
		RetryAttempts:  2,                // Reduced retries for faster failure detection
		QueryTimeout:   30 * time.Minute, // Extended timeout for large batches
	}

	log.Printf("Starting high-performance loader with configuration:")
	log.Printf("- File workers: %d", config.MaxWorkers)
	log.Printf("- Batch workers: %d", config.BatchInsertWorkers)
	log.Printf("- Connections: %d", config.MaxConnections)
	log.Printf("- Chunk size: %s rows", formatNumber(int64(config.ChunkSize)))
	log.Printf("- Memory limit: %d GB", config.MemoryLimit/(1024*1024*1024))

	startDateStr := "2025-09-15"
	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		panic(err)
	}

	n := 8

	var targetDates []string
	for i := 0; i < n; i++ {
		date := startDate.AddDate(0, 0, -i) // subtract i days
		targetDates = append(targetDates, date.Format("20060102"))
	}

	parquetFolders := []string{}
	for _, date := range targetDates {
		parquetFolders = append(parquetFolders, fmt.Sprintf("/mnt/blobcontainer/load_date=%s", date))
	}

	loader, err := NewParquetToClickHouseLoader(config)
	if err != nil {
		log.Fatalf("Failed to create loader: %v", err)
	}
	defer loader.Close()

	if err := loader.setupDatabase(); err != nil {
		log.Fatalf("Database setup failed: %v", err)
	}

	log.Println("🚀 Starting high-performance parallel data loading...")

	if err := loader.loadParquetFilesParallel(parquetFolders); err != nil {
		log.Fatalf("Parallel loading failed: %v", err)
	}

	if err := loader.optimizeTable(); err != nil {
		log.Printf("Table optimization failed: %v", err)
	}

	log.Println("🎉 HIGH-PERFORMANCE DATA LOADING COMPLETED SUCCESSFULLY!")
	log.Println("💪 Maximum system resources utilized for optimal throughput!")
}
