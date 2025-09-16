package main

import (
	"context"
	"database/sql"
	"fmt"
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
)

type LoaderConfig struct {
	CHHost         string
	CHPort         int
	CHDatabase     string
	CHUser         string
	CHPassword     string
	MaxWorkers     int
	ChunkSize      int
	ResumeFromFile int
	ProgressFile   string
	RetryAttempts  int
	QueryTimeout   time.Duration
}

type LoaderStats struct {
	TotalFilesProcessed int64
	TotalRowsInserted   int64
	FailedFiles         []string
	SkippedFiles        []string
	StartTime           time.Time
	EndTime             time.Time
	ProcessedFiles      map[string]bool
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

func (s *LoaderStats) MarkProcessed(filePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ProcessedFiles[filePath] = true
}

func (s *LoaderStats) IsProcessed(filePath string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ProcessedFiles[filePath]
}

func (s *LoaderStats) GetStats() (int64, int64, []string, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalFilesProcessed, s.TotalRowsInserted, s.FailedFiles, s.SkippedFiles
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
	config LoaderConfig
	stats  *LoaderStats
}

func NewParquetToClickHouseLoader(config LoaderConfig) *ParquetToClickHouseLoader {
	return &ParquetToClickHouseLoader{
		config: config,
		stats: &LoaderStats{
			StartTime:      time.Now(),
			ProcessedFiles: make(map[string]bool),
		},
	}
}

func (loader *ParquetToClickHouseLoader) getClickHouseURL() string {
	// Use the working connection string from our test
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		loader.config.CHHost, loader.config.CHPort,
		loader.config.CHUser, loader.config.CHPassword, loader.config.CHDatabase)
}

func (loader *ParquetToClickHouseLoader) loadProgress() error {
	if loader.config.ProgressFile == "" {
		return nil
	}

	if _, err := os.Stat(loader.config.ProgressFile); os.IsNotExist(err) {
		return nil
	}

	content, err := os.ReadFile(loader.config.ProgressFile)
	if err != nil {
		return fmt.Errorf("failed to read progress file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			loader.stats.ProcessedFiles[line] = true
		}
	}

	log.Printf("Loaded progress: %d files already processed", len(loader.stats.ProcessedFiles))
	return nil
}

func (loader *ParquetToClickHouseLoader) saveProgress(filePath string) error {
	if loader.config.ProgressFile == "" {
		return nil
	}

	file, err := os.OpenFile(loader.config.ProgressFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open progress file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(filePath + "\n")
	return err
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
	if loader.stats.IsProcessed(filePath) {
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
		result.Success = true
		loader.stats.MarkProcessed(filePath)
		loader.saveProgress(filePath)
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

	result.RowsProcessed = rowsInserted
	result.Success = true
	result.ProcessingTime = time.Since(result.StartTime)

	loader.stats.MarkProcessed(filePath)
	loader.saveProgress(filePath)

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
	if err := loader.loadProgress(); err != nil {
		log.Printf("Warning: Could not load progress: %v", err)
	}

	go loader.monitorResources()

	loader.stats.StartTime = time.Now()
	log.Printf("Starting parallel data loading with %d workers...", loader.config.MaxWorkers)

	allFiles, err := loader.getParquetFiles(parquetFolders)
	if err != nil {
		return fmt.Errorf("failed to get parquet files: %w", err)
	}

	if len(allFiles) == 0 {
		log.Println("No parquet files found to process!")
		return nil
	}

	var filesToProcess []struct {
		path     string
		loadDate time.Time
	}

	startIndex := 0
	if loader.config.ResumeFromFile > 0 {
		startIndex = loader.config.ResumeFromFile - 1
		log.Printf("Resuming from file index: %d", startIndex)
	}

	for i := startIndex; i < len(allFiles); i++ {
		if !loader.stats.IsProcessed(allFiles[i].path) {
			filesToProcess = append(filesToProcess, allFiles[i])
		}
	}

	log.Printf("Files to process: %d (skipping %d already processed)", len(filesToProcess), len(allFiles)-len(filesToProcess))

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
	log.Printf("Total rows inserted: %s", formatNumber(totalRows))

	if totalRows > 0 && duration.Seconds() > 0 {
		rowsPerSecond := float64(totalRows) / duration.Seconds()
		log.Printf("Average insertion rate: %s rows/second", formatNumber(int64(rowsPerSecond)))
	}

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
		CHHost:         "localhost",
		CHPort:         9000, // Use 8123 for HTTP interface
		CHDatabase:     "device_tracking",
		CHUser:         "default",
		CHPassword:     "nyros",
		MaxWorkers:     runtime.NumCPU() / 2,
		ChunkSize:      100000,
		ResumeFromFile: 2366,
		ProgressFile:   "/tmp/parquet_loader_progress.txt",
		RetryAttempts:  3,
		QueryTimeout:   5 * time.Minute,
	}

	parquetFolders := []string{
		"/mnt/blobcontainer/load_date=20250823",
		//"/mnt/blobcontainer/load_date=20250822",
		//"/mnt/blobcontainer/load_date=20250821",
	}

	loader := NewParquetToClickHouseLoader(config)

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
