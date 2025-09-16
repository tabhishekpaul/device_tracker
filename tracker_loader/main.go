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
	_ "github.com/mattn/go-sqlite3"
)

type LoaderConfig struct {
	CHHost       string
	CHPort       int
	CHDatabase   string
	CHUser       string
	CHPassword   string
	MaxWorkers   int
	ChunkSize    int
	QueryTimeout time.Duration
	SQLiteDB     string
}

type LoaderStats struct {
	TotalFilesProcessed int64
	TotalRowsInserted   int64
	FailedFiles         []string
	SkippedFiles        []string
	StartTime           time.Time
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

type FileJob struct {
	FilePath string
	LoadDate time.Time
}

type FileResult struct {
	FilePath      string
	RowsProcessed int64
	Success       bool
	Error         error
	Duration      time.Duration
}

type ParquetLoader struct {
	config   LoaderConfig
	stats    *LoaderStats
	sqliteDB *sql.DB
}

func NewParquetLoader(config LoaderConfig) *ParquetLoader {
	return &ParquetLoader{
		config: config,
		stats: &LoaderStats{
			StartTime: time.Now(),
		},
	}
}

func (loader *ParquetLoader) initSQLiteTracking() error {
	var err error
	loader.sqliteDB, err = sql.Open("sqlite3", loader.config.SQLiteDB)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Create tracking table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS processed_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filename TEXT UNIQUE NOT NULL,
		filepath TEXT NOT NULL,
		rows_inserted INTEGER NOT NULL,
		processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`

	_, err = loader.sqliteDB.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create SQLite tracking table: %w", err)
	}

	log.Println("✅ SQLite tracking database initialized")
	return nil
}

func (loader *ParquetLoader) isFileProcessed(filename string) (bool, error) {
	var count int
	err := loader.sqliteDB.QueryRow("SELECT COUNT(*) FROM processed_files WHERE filename = ?", filename).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (loader *ParquetLoader) markFileProcessed(filename, filepath string, rowsInserted int64) error {
	_, err := loader.sqliteDB.Exec(
		"INSERT INTO processed_files (filename, filepath, rows_inserted) VALUES (?, ?, ?)",
		filename, filepath, rowsInserted,
	)
	return err
}

func (loader *ParquetLoader) getClickHouseURL() string {
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		loader.config.CHHost, loader.config.CHPort,
		loader.config.CHUser, loader.config.CHPassword, loader.config.CHDatabase)
}

func (loader *ParquetLoader) setupClickHouse() error {
	log.Println("Setting up ClickHouse database and tables...")

	conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Test connection
	var testResult int
	err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
	if err != nil {
		return fmt.Errorf("failed to test connection: %w", err)
	}
	log.Println("✅ ClickHouse connection successful")

	// Create database if not exists
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", loader.config.CHDatabase))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table if not exists
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS device_data (
		device_id LowCardinality(String),
		event_timestamp DateTime64(3),
		latitude Float64,
		longitude Float64,
		load_date Date
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMM(load_date)
	ORDER BY (load_date, cityHash64(device_id), event_timestamp)
	SETTINGS index_granularity = 8192`

	_, err = conn.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("✅ ClickHouse table 'device_data' ready")
	return nil
}

func (loader *ParquetLoader) getParquetFiles(folders []string) ([]FileJob, error) {
	var allFiles []FileJob

	for _, folder := range folders {
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			log.Printf("⚠️  Folder does not exist: %s", folder)
			continue
		}

		// Extract load date from folder name
		folderName := filepath.Base(folder)
		if !strings.Contains(folderName, "load_date=") {
			return nil, fmt.Errorf("folder name doesn't contain load_date: %s", folder)
		}

		dateStr := strings.Split(folderName, "=")[1]
		loadDate, err := time.Parse("20060102", dateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid date format in folder: %s", folder)
		}

		// Find all parquet files
		pattern := filepath.Join(folder, "*.parquet")
		files, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("error scanning folder %s: %w", folder, err)
		}

		for _, filePath := range files {
			allFiles = append(allFiles, FileJob{
				FilePath: filePath,
				LoadDate: loadDate,
			})
		}

		log.Printf("📁 Found %d parquet files in %s", len(files), folder)
	}

	log.Printf("📊 Total parquet files found: %d", len(allFiles))
	return allFiles, nil
}

func (loader *ParquetLoader) processFile(job FileJob) FileResult {
	start := time.Now()
	result := FileResult{FilePath: job.FilePath}
	filename := filepath.Base(job.FilePath)

	// Check if file already processed
	processed, err := loader.isFileProcessed(filename)
	if err != nil {
		result.Error = fmt.Errorf("failed to check if file processed: %w", err)
		return result
	}

	if processed {
		log.Printf("⏭️  Skipping already processed file: %s", filename)
		loader.stats.AddSkippedFile(job.FilePath)
		result.Success = true
		result.Duration = time.Since(start)
		return result
	}

	log.Printf("🔄 Processing: %s", filename)

	// Open and read parquet file
	osFile, err := os.Open(job.FilePath)
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

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create arrow reader: %w", err)
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
		log.Printf("⚠️  Empty file: %s", filename)
		// Mark empty file as processed too
		loader.markFileProcessed(filename, job.FilePath, 0)
		result.Success = true
		result.Duration = time.Since(start)
		return result
	}

	// Insert data into ClickHouse
	rowsInserted, err := loader.insertData(table, job.LoadDate)
	if err != nil {
		result.Error = fmt.Errorf("failed to insert data: %w", err)
		return result
	}

	// Mark file as processed in SQLite
	err = loader.markFileProcessed(filename, job.FilePath, rowsInserted)
	if err != nil {
		result.Error = fmt.Errorf("failed to mark file as processed: %w", err)
		return result
	}

	result.RowsProcessed = rowsInserted
	result.Success = true
	result.Duration = time.Since(start)

	log.Printf("✅ Completed: %s - %d rows in %.2fs",
		filename, rowsInserted, result.Duration.Seconds())

	return result
}

func (loader *ParquetLoader) insertData(table arrow.Table, loadDate time.Time) (int64, error) {
	conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
	if err != nil {
		return 0, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Find column indices with case-insensitive matching
	schema := table.Schema()
	deviceIDIdx, timestampIdx, latIdx, lonIdx := -1, -1, -1, -1

	for i, field := range schema.Fields() {
		fieldName := strings.ToLower(field.Name)
		switch {
		case fieldName == "device_id" || fieldName == "deviceid":
			deviceIDIdx = i
		case fieldName == "event_timestamp" || fieldName == "timestamp" || fieldName == "event_time" || fieldName == "time":
			timestampIdx = i
		case fieldName == "latitude" || fieldName == "lat":
			latIdx = i
		case fieldName == "longitude" || fieldName == "lon" || fieldName == "lng":
			lonIdx = i
		}
	}

	if deviceIDIdx == -1 || timestampIdx == -1 || latIdx == -1 || lonIdx == -1 {
		// Debug: print available columns
		var availableColumns []string
		for _, field := range schema.Fields() {
			availableColumns = append(availableColumns, field.Name)
		}
		log.Printf("Available columns: %v", availableColumns)
		return 0, fmt.Errorf("required columns not found - need: device_id, event_timestamp, latitude, longitude")
	}

	// Get columns
	deviceIDCol := table.Column(deviceIDIdx)
	timestampCol := table.Column(timestampIdx)
	latCol := table.Column(latIdx)
	lonCol := table.Column(lonIdx)

	// Process data in chunks
	numRows := int(table.NumRows())
	chunkSize := loader.config.ChunkSize
	var totalInserted int64

	for start := 0; start < numRows; start += chunkSize {
		end := start + chunkSize
		if end > numRows {
			end = numRows
		}

		var values []string
		var args []interface{}

		for i := start; i < end; i++ {
			deviceID := loader.getStringValue(deviceIDCol, i)
			timestamp := loader.getTimestampValue(timestampCol, i)
			latitude := loader.getFloat64Value(latCol, i)
			longitude := loader.getFloat64Value(lonCol, i)

			// Enhanced validation
			if deviceID == "" || timestamp.IsZero() {
				continue
			}

			// Validate timestamp is reasonable (not too far in past/future)
			now := time.Now()
			if timestamp.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) ||
				timestamp.After(now.Add(24*time.Hour)) {
				log.Printf("⚠️  Invalid timestamp detected: %v, skipping row", timestamp)
				continue
			}

			// Validate coordinates
			if latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
				continue
			}

			values = append(values, "(?, ?, ?, ?, ?)")
			args = append(args, deviceID, timestamp, latitude, longitude, loadDate)
		}

		if len(values) == 0 {
			continue
		}

		query := fmt.Sprintf(`INSERT INTO device_data 
			(device_id, event_timestamp, latitude, longitude, load_date) 
			VALUES %s`, strings.Join(values, ","))

		_, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to insert batch: %w", err)
		}

		totalInserted += int64(len(values))
	}

	return totalInserted, nil
}

// Helper functions for extracting values from Arrow columns
func (loader *ParquetLoader) getStringValue(col *arrow.Column, row int) string {
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

func (loader *ParquetLoader) getTimestampValue(col *arrow.Column, row int) time.Time {
	if col.Len() <= row {
		return time.Time{}
	}

	chunk := col.Data().Chunk(0)
	if chunk.IsNull(row) {
		return time.Time{}
	}

	switch arr := chunk.(type) {
	case *array.Timestamp:
		// Handle different timestamp units
		unit := arr.DataType().(*arrow.TimestampType).Unit
		timestamp := arr.Value(row)
		return timestamp.ToTime(unit)
	case *array.Int64:
		// Handle epoch milliseconds stored as int64
		epochMillis := arr.Value(row)
		if epochMillis <= 0 {
			return time.Time{}
		}
		// Convert milliseconds to time
		return time.Unix(epochMillis/1000, (epochMillis%1000)*1000000).UTC()
	case *array.Uint64:
		// Handle epoch milliseconds stored as uint64
		epochMillis := int64(arr.Value(row))
		if epochMillis <= 0 {
			return time.Time{}
		}
		return time.Unix(epochMillis/1000, (epochMillis%1000)*1000000).UTC()
	case *array.Int32:
		// Handle epoch seconds stored as int32
		epochSecs := int64(arr.Value(row))
		if epochSecs <= 0 {
			return time.Time{}
		}
		return time.Unix(epochSecs, 0).UTC()
	case *array.String:
		// Handle timestamp as string (try to parse)
		timeStr := arr.Value(row)
		if timeStr == "" {
			return time.Time{}
		}
		// Try different timestamp formats
		formats := []string{
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			time.RFC3339,
			time.RFC3339Nano,
		}
		for _, format := range formats {
			if t, err := time.Parse(format, timeStr); err == nil {
				return t.UTC()
			}
		}
		log.Printf("⚠️  Could not parse timestamp string: %s", timeStr)
		return time.Time{}
	default:
		log.Printf("⚠️  Unsupported timestamp type: %T", arr)
		return time.Time{}
	}
}

func (loader *ParquetLoader) getFloat64Value(col *arrow.Column, row int) float64 {
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

func (loader *ParquetLoader) loadFiles(folders []string) error {
	log.Printf("🚀 Starting data loading with %d workers...", loader.config.MaxWorkers)

	// Get all files to process
	allFiles, err := loader.getParquetFiles(folders)
	if err != nil {
		return fmt.Errorf("failed to get parquet files: %w", err)
	}

	if len(allFiles) == 0 {
		log.Println("❌ No parquet files found to process!")
		return nil
	}

	// Create channels for work distribution
	jobsChan := make(chan FileJob, len(allFiles))
	resultsChan := make(chan FileResult, len(allFiles))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < loader.config.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobsChan {
				result := loader.processFile(job)
				resultsChan <- result
			}
		}(i)
	}

	// Send jobs to workers
	go func() {
		for _, job := range allFiles {
			jobsChan <- job
		}
		close(jobsChan)
	}()

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	completed := 0
	for result := range resultsChan {
		completed++

		if result.Success {
			loader.stats.IncrementProcessed()
			loader.stats.AddRows(result.RowsProcessed)
		} else {
			loader.stats.AddFailedFile(result.FilePath)
			log.Printf("❌ Failed: %s - %v", filepath.Base(result.FilePath), result.Error)
		}

		if completed%100 == 0 || completed == len(allFiles) {
			log.Printf("📈 Progress: %d/%d files completed", completed, len(allFiles))
		}
	}

	loader.printStats()
	return nil
}

func (loader *ParquetLoader) printStats() {
	duration := time.Since(loader.stats.StartTime)

	log.Println("=" + strings.Repeat("=", 60))
	log.Println("📊 LOADING COMPLETED")
	log.Println("=" + strings.Repeat("=", 60))
	log.Printf("⏱️  Total time: %v", duration)
	log.Printf("📁 Files processed: %d", loader.stats.TotalFilesProcessed)
	log.Printf("⏭️  Files skipped: %d", len(loader.stats.SkippedFiles))
	log.Printf("📝 Total rows inserted: %s", formatNumber(loader.stats.TotalRowsInserted))
	log.Printf("❌ Failed files: %d", len(loader.stats.FailedFiles))

	if loader.stats.TotalRowsInserted > 0 && duration.Seconds() > 0 {
		rowsPerSecond := float64(loader.stats.TotalRowsInserted) / duration.Seconds()
		log.Printf("⚡ Insertion rate: %s rows/second", formatNumber(int64(rowsPerSecond)))
	}

	if len(loader.stats.FailedFiles) > 0 {
		log.Println("\n❌ Failed files:")
		for _, failedFile := range loader.stats.FailedFiles {
			log.Printf("   - %s", filepath.Base(failedFile))
		}
	}
}

func (loader *ParquetLoader) Close() error {
	if loader.sqliteDB != nil {
		return loader.sqliteDB.Close()
	}
	return nil
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
		CHHost:       "localhost",
		CHPort:       9000,
		CHDatabase:   "device_tracking",
		CHUser:       "default",
		CHPassword:   "nyros",
		MaxWorkers:   runtime.NumCPU() / 2,
		ChunkSize:    50000,
		QueryTimeout: 5 * time.Minute,
		SQLiteDB:     "./parquet_loader_tracking.db",
	}

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

	loader := NewParquetLoader(config)
	defer loader.Close()

	// Initialize SQLite tracking
	if err := loader.initSQLiteTracking(); err != nil {
		log.Fatalf("❌ SQLite tracking setup failed: %v", err)
	}

	// Setup ClickHouse database
	if err := loader.setupClickHouse(); err != nil {
		log.Fatalf("❌ ClickHouse setup failed: %v", err)
	}

	// Load files
	if err := loader.loadFiles(parquetFolders); err != nil {
		log.Fatalf("❌ Loading failed: %v", err)
	}

	log.Println("🎉 All data successfully loaded into ClickHouse!")
}
