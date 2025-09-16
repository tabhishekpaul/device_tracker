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

type ParquetToClickHouseLoader struct {
	config  LoaderConfig
	stats   *LoaderStats
	localDB *sql.DB
}

func NewParquetToClickHouseLoader(config LoaderConfig) (*ParquetToClickHouseLoader, error) {
	// Initialize local SQLite database
	localDB, err := sql.Open("sqlite3", config.LocalDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local database: %w", err)
	}

	loader := &ParquetToClickHouseLoader{
		config:  config,
		localDB: localDB,
		stats: &LoaderStats{
			StartTime: time.Now(),
		},
	}

	if err := loader.initLocalDB(); err != nil {
		return nil, fmt.Errorf("failed to initialize local database: %w", err)
	}

	return loader, nil
}

func (loader *ParquetToClickHouseLoader) initLocalDB() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS processed_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
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
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
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
	// Use the working connection string from our test
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		loader.config.CHHost, loader.config.CHPort,
		loader.config.CHUser, loader.config.CHPassword, loader.config.CHDatabase)
}

func (loader *ParquetToClickHouseLoader) setupDatabase() error {
	log.Println("Setting up ClickHouse database and tables...")

	// Debug: print the connection string (without password)
	connStr := loader.getClickHouseURL()
	debugConnStr := strings.Replace(connStr, loader.config.CHPassword, "***", 1)
	log.Printf("Using connection string: %s", debugConnStr)

	conn, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Test connection first
	var testResult int
	err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
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
                max_compress_block_size = 2097152,
                min_compress_block_size = 65536,
                max_insert_block_size = 1048576
        `

	_, err = conn.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("ClickHouse table 'device_data' created successfully")
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

	for _, folder := range baseFolders {
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			log.Printf("Warning: Folder does not exist: %s", folder)
			continue
		}

		folderName := filepath.Base(folder)
		if !strings.Contains(folderName, "load_date=") {
			log.Printf("Error: Folder name doesn't contain load_date: %s", folder)
			continue
		}

		dateStr := strings.Split(folderName, "=")[1]
		loadDate, err := time.Parse("20060102", dateStr)
		if err != nil {
			log.Printf("Error: Invalid date format in folder: %s", folder)
			continue
		}

		pattern := filepath.Join(folder, "*.parquet")
		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("Error scanning folder %s: %v", folder, err)
			continue
		}

		for _, filePath := range files {
			allFiles = append(allFiles, struct {
				path     string
				loadDate time.Time
			}{filePath, loadDate})
		}

		log.Printf("Found %d parquet files in %s", len(files), folder)
	}

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

	// Open parquet file
	osFile, err := os.Open(filePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %w", err)
		return result
	}
	defer osFile.Close()

	// Create parquet reader - simplified version
	pf, err := file.NewParquetReader(osFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to create parquet reader: %w", err)
		return result
	}
	defer pf.Close()

	// Create Arrow reader - use basic constructor
	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
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
	conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
	if err != nil {
		return 0, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

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

	// Process data in smaller chunks
	numRows := int(table.NumRows())
	chunkSize := loader.config.ChunkSize / 2
	var totalInserted int64

	for start := 0; start < numRows; start += chunkSize {
		end := start + chunkSize
		if end > numRows {
			end = numRows
		}

		var values []string
		var args []interface{}
		validRows := 0

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

			values = append(values, "(?, ?, ?, ?, ?)")
			args = append(args, deviceID, timestamp, latitude, longitude, loadDate)
			validRows++
		}

		if validRows == 0 {
			continue
		}

		query := fmt.Sprintf(`
                        INSERT INTO device_data 
                        (device_id, event_timestamp, latitude, longitude, load_date) 
                        VALUES %s`, strings.Join(values, ","))

		_, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to insert batch: %w", err)
		}

		totalInserted += int64(validRows)

		if numRows > chunkSize && start%50000 == 0 {
			log.Printf("Inserted %d/%d rows", start+validRows, numRows)
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
	go loader.monitorResources()

	loader.stats.StartTime = time.Now()
	log.Printf("Starting parallel data loading with %d workers...", loader.config.MaxWorkers)

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

	// Filter files that need processing
	var filesToProcess []struct {
		path     string
		loadDate time.Time
	}

	skippedCount := 0
	for _, file := range allFiles {
		isProcessed, _, err := loader.isFileProcessed(file.path)
		if err != nil {
			log.Printf("Error checking file %s: %v", filepath.Base(file.path), err)
			continue
		}

		if !isProcessed {
			filesToProcess = append(filesToProcess, file)
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
			log.Printf("✅ (%d/%d) Completed: %s - %d rows in %.2fs%s",
				completed, len(filesToProcess), filepath.Base(result.FilePath),
				result.RowsProcessed, result.ProcessingTime.Seconds(), retryStatus)
		} else {
			loader.stats.AddFailedFile(result.FilePath)
			log.Printf("❌ (%d/%d) Failed: %s - %v",
				completed, len(filesToProcess), filepath.Base(result.FilePath), result.Error)
		}
	}

	loader.stats.EndTime = time.Now()
	loader.printFinalStats()
	return nil
}

func (loader *ParquetToClickHouseLoader) printFinalStats() {
	duration := loader.stats.EndTime.Sub(loader.stats.StartTime)
	processed, totalRows, failedFiles, skippedFiles := loader.stats.GetStats()

	log.Println("=" + strings.Repeat("=", 80))
	log.Println("DATA LOADING COMPLETED")
	log.Println("=" + strings.Repeat("=", 80))
	log.Printf("Total processing time: %v", duration)
	log.Printf("Files processed successfully: %d", processed)
	log.Printf("Files skipped (already processed): %d", len(skippedFiles))
	log.Printf("Files failed: %d", len(failedFiles))
	log.Printf("New rows inserted this run: %s", formatNumber(totalRows))

	if totalRows > 0 && duration.Seconds() > 0 {
		rowsPerSecond := float64(totalRows) / duration.Seconds()
		log.Printf("Average insertion rate: %s rows/second", formatNumber(int64(rowsPerSecond)))
	}

	// Get total stats from tracking database
	totalFiles, totalRecords, _ := loader.getProcessedFilesStats()
	log.Printf("Total files in tracking DB: %d", totalFiles)
	log.Printf("Total records in tracking DB: %s", formatNumber(totalRecords))

	if len(failedFiles) > 0 {
		log.Println("\nFailed files:")
		for _, failedFile := range failedFiles {
			log.Printf("  - %s", filepath.Base(failedFile))
		}
	}

	loader.verifyData()
}

func (loader *ParquetToClickHouseLoader) verifyData() {
	conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
	if err != nil {
		log.Printf("Failed to connect for verification: %v", err)
		return
	}
	defer conn.Close()

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

	log.Println("\nClickHouse verification:")
	log.Printf("Total rows in database: %s", formatNumber(totalCount))
	log.Printf("Unique devices: %s", formatNumber(uniqueDevices))
	log.Printf("Date range: %s to %s", minDate, maxDate)
}

func (loader *ParquetToClickHouseLoader) optimizeTable() error {
	log.Println("Optimizing ClickHouse table...")

	conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
	if err != nil {
		return fmt.Errorf("failed to connect for optimization: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	_, err = conn.ExecContext(ctx, "OPTIMIZE TABLE device_data")
	if err != nil {
		return fmt.Errorf("failed to optimize table: %w", err)
	}

	log.Println("Table optimization completed")
	return nil
}

func (loader *ParquetToClickHouseLoader) monitorResources() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		log.Printf("📊 Memory: %.2f MB | Goroutines: %d | GC Cycles: %d",
			float64(m.Alloc)/1024/1024, runtime.NumGoroutine(), m.NumGC)

		if m.Alloc > 2*1024*1024*1024 { // 2GB
			runtime.GC()
			log.Println("🧹 Forced garbage collection due to high memory usage")
		}
	}
}

func (loader *ParquetToClickHouseLoader) Close() {
	if loader.localDB != nil {
		loader.localDB.Close()
	}
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
	config := LoaderConfig{
		CHHost:        "localhost",
		CHPort:        9000, // Use 8123 for HTTP interface
		CHDatabase:    "device_tracking",
		CHUser:        "default",
		CHPassword:    "nyros",
		MaxWorkers:    runtime.NumCPU() / 2,
		ChunkSize:     100000,
		LocalDBPath:   "./parquet_tracking.db", // SQLite database for tracking
		RetryAttempts: 3,
		QueryTimeout:  5 * time.Minute,
	}

	parquetFolders := []string{
		"/mnt/blobcontainer/load_date=20250823",
		//"/mnt/blobcontainer/load_date=20250822",
		//"/mnt/blobcontainer/load_date=20250821",
	}

	loader, err := NewParquetToClickHouseLoader(config)
	if err != nil {
		log.Fatalf("Failed to create loader: %v", err)
	}
	defer loader.Close()

	if err := loader.setupDatabase(); err != nil {
		log.Fatalf("Database setup failed: %v", err)
	}

	if err := loader.loadParquetFilesParallel(parquetFolders); err != nil {
		log.Fatalf("Parallel loading failed: %v", err)
	}

	if err := loader.optimizeTable(); err != nil {
		log.Printf("Table optimization failed: %v", err)
	}

	log.Println("🎉 All data successfully loaded into ClickHouse!")
}
