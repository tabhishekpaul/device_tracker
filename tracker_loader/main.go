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
	"sync/atomic"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	_ "github.com/mattn/go-sqlite3"
)

type LoaderConfig struct {
	CHHost         string
	CHPort         int
	CHDatabase     string
	CHUser         string
	CHPassword     string
	MaxWorkers     int
	ChunkSize      int
	QueryTimeout   time.Duration
	SQLiteDB       string
	BatchInserts   bool
	ConnectionPool int
	RetryAttempts  int
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
	atomic.AddInt64(&s.TotalFilesProcessed, 1)
}

func (s *LoaderStats) AddRows(count int64) {
	atomic.AddInt64(&s.TotalRowsInserted, count)
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

// In-memory tracking to eliminate SQLite locks
type FileTracker struct {
	processedFiles map[string]bool
	mu             sync.RWMutex
}

func NewFileTracker() *FileTracker {
	return &FileTracker{
		processedFiles: make(map[string]bool),
	}
}

func (ft *FileTracker) IsProcessed(filename string) bool {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	return ft.processedFiles[filename]
}

func (ft *FileTracker) MarkProcessed(filename string) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.processedFiles[filename] = true
}

func (ft *FileTracker) LoadFromSQLite(db *sql.DB) error {
	rows, err := db.Query("SELECT filename FROM processed_files")
	if err != nil {
		return err
	}
	defer rows.Close()

	ft.mu.Lock()
	defer ft.mu.Unlock()

	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			continue
		}
		ft.processedFiles[filename] = true
	}

	log.Printf("📋 Loaded %d previously processed files into memory", len(ft.processedFiles))
	return nil
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

type BatchInsertData struct {
	DeviceIDs  []string
	Timestamps []time.Time
	Latitudes  []float64
	Longitudes []float64
	LoadDate   time.Time
}

type ParquetLoader struct {
	config      LoaderConfig
	stats       *LoaderStats
	sqliteDB    *sql.DB
	fileTracker *FileTracker
	chConnPool  chan *sql.DB
	sqliteMutex sync.Mutex
}

func NewParquetLoader(config LoaderConfig) *ParquetLoader {
	return &ParquetLoader{
		config:      config,
		stats:       &LoaderStats{StartTime: time.Now()},
		fileTracker: NewFileTracker(),
		chConnPool:  make(chan *sql.DB, config.ConnectionPool),
	}
}

func (loader *ParquetLoader) initSQLiteTracking() error {
	var err error
	// Enable WAL mode and optimize SQLite for concurrent reads
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=10000&_busy_timeout=30000", loader.config.SQLiteDB)
	loader.sqliteDB, err = sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Set connection limits
	loader.sqliteDB.SetMaxOpenConns(1) // Single connection for SQLite
	loader.sqliteDB.SetMaxIdleConns(1)

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

	// Load existing processed files into memory
	if err := loader.fileTracker.LoadFromSQLite(loader.sqliteDB); err != nil {
		log.Printf("⚠️ Warning: Could not load processed files from SQLite: %v", err)
	}

	log.Println("✅ SQLite tracking database initialized with in-memory caching")
	return nil
}

func (loader *ParquetLoader) initClickHousePool() error {
	log.Printf("🔄 Initializing ClickHouse connection pool with %d connections...", loader.config.ConnectionPool)

	for i := 0; i < loader.config.ConnectionPool; i++ {
		conn, err := sql.Open("clickhouse", loader.getClickHouseURL())
		if err != nil {
			return fmt.Errorf("failed to create ClickHouse connection %d: %w", i, err)
		}

		// Test connection
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var testResult int
		err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
		cancel()

		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to test ClickHouse connection %d: %w", i, err)
		}

		loader.chConnPool <- conn
	}

	log.Printf("✅ ClickHouse connection pool ready with %d connections", loader.config.ConnectionPool)
	return nil
}

func (loader *ParquetLoader) getClickHouseConnection() *sql.DB {
	return <-loader.chConnPool
}

func (loader *ParquetLoader) returnClickHouseConnection(conn *sql.DB) {
	loader.chConnPool <- conn
}

func (loader *ParquetLoader) markFileProcessedAsync(filename, filepath string, rowsInserted int64) {
	go func() {
		loader.sqliteMutex.Lock()
		defer loader.sqliteMutex.Unlock()

		// Mark in memory first
		loader.fileTracker.MarkProcessed(filename)

		// Then persist to SQLite asynchronously
		_, err := loader.sqliteDB.Exec(
			"INSERT OR REPLACE INTO processed_files (filename, filepath, rows_inserted) VALUES (?, ?, ?)",
			filename, filepath, rowsInserted,
		)
		if err != nil {
			log.Printf("⚠️ Warning: Failed to persist to SQLite: %v", err)
		}
	}()
}

func (loader *ParquetLoader) getClickHouseURL() string {
	return fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&max_execution_time=0&send_timeout=300&receive_timeout=300",
		loader.config.CHHost, loader.config.CHPort,
		loader.config.CHUser, loader.config.CHPassword, loader.config.CHDatabase)
}

func (loader *ParquetLoader) setupClickHouse() error {
	log.Println("Setting up ClickHouse database and tables...")

	conn := loader.getClickHouseConnection()
	defer loader.returnClickHouseConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Create database if not exists
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", loader.config.CHDatabase))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Optimized table schema for high-speed inserts
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
	SETTINGS index_granularity = 8192,
			 max_parts_in_total = 10000,
			 parts_to_delay_insert = 300,
			 parts_to_throw_insert = 3000,
			 max_insert_block_size = 1048576`

	_, err = conn.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("✅ ClickHouse table 'device_data' ready with optimized settings")
	return nil
}

func (loader *ParquetLoader) getParquetFiles(folders []string) ([]FileJob, error) {
	var allFiles []FileJob

	for _, folder := range folders {
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			log.Printf("⚠️  Folder does not exist: %s", folder)
			continue
		}

		folderName := filepath.Base(folder)
		if !strings.Contains(folderName, "load_date=") {
			return nil, fmt.Errorf("folder name doesn't contain load_date: %s", folder)
		}

		dateStr := strings.Split(folderName, "=")[1]
		loadDate, err := time.Parse("20060102", dateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid date format in folder: %s", folder)
		}

		pattern := filepath.Join(folder, "*.parquet")
		files, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("error scanning folder %s: %w", folder, err)
		}

		for _, filePath := range files {
			filename := filepath.Base(filePath)
			if !loader.fileTracker.IsProcessed(filename) {
				allFiles = append(allFiles, FileJob{
					FilePath: filePath,
					LoadDate: loadDate,
				})
			}
		}

		log.Printf("📁 Found %d unprocessed parquet files in %s", len(files), folder)
	}

	log.Printf("📊 Total unprocessed parquet files: %d", len(allFiles))
	return allFiles, nil
}

func (loader *ParquetLoader) processFile(job FileJob) FileResult {
	start := time.Now()
	result := FileResult{FilePath: job.FilePath}
	filename := filepath.Base(job.FilePath)

	// Double-check in-memory tracking
	if loader.fileTracker.IsProcessed(filename) {
		log.Printf("⏭️  Skipping already processed file: %s", filename)
		loader.stats.AddSkippedFile(job.FilePath)
		result.Success = true
		result.Duration = time.Since(start)
		return result
	}

	log.Printf("🔄 Processing: %s", filename)

	// Process with retry logic
	for attempt := 1; attempt <= loader.config.RetryAttempts; attempt++ {
		rowsInserted, err := loader.processFileAttempt(job)
		if err == nil {
			// Success
			loader.markFileProcessedAsync(filename, job.FilePath, rowsInserted)
			result.RowsProcessed = rowsInserted
			result.Success = true
			result.Duration = time.Since(start)
			log.Printf("✅ Completed: %s - %s rows in %.2fs",
				filename, formatNumber(rowsInserted), result.Duration.Seconds())
			return result
		}

		if attempt < loader.config.RetryAttempts {
			log.Printf("⚠️  Attempt %d failed for %s: %v. Retrying...", attempt, filename, err)
			time.Sleep(time.Duration(attempt) * time.Second)
		} else {
			result.Error = err
			log.Printf("❌ Failed after %d attempts: %s - %v", loader.config.RetryAttempts, filename, err)
		}
	}

	return result
}

func (loader *ParquetLoader) processFileAttempt(job FileJob) (int64, error) {
	// Open and read parquet file
	osFile, err := os.Open(job.FilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer osFile.Close()

	pf, err := file.NewParquetReader(osFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pf.Close()

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	numRows := int(table.NumRows())
	if numRows == 0 {
		return 0, nil
	}

	// Use batch insert for better performance
	if loader.config.BatchInserts {
		return loader.batchInsertData(table, job.LoadDate)
	}
	return loader.insertData(table, job.LoadDate)
}

func (loader *ParquetLoader) batchInsertData(table arrow.Table, loadDate time.Time) (int64, error) {
	conn := loader.getClickHouseConnection()
	defer loader.returnClickHouseConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

	// Extract all data into batches
	batchData, err := loader.extractTableData(table, loadDate)
	if err != nil {
		return 0, err
	}

	if len(batchData.DeviceIDs) == 0 {
		return 0, nil
	}

	// Prepare batch insert
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO device_data 
		(device_id, event_timestamp, latitude, longitude, load_date) 
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert in chunks
	chunkSize := loader.config.ChunkSize
	totalInserted := int64(0)

	for i := 0; i < len(batchData.DeviceIDs); i += chunkSize {
		end := i + chunkSize
		if end > len(batchData.DeviceIDs) {
			end = len(batchData.DeviceIDs)
		}

		for j := i; j < end; j++ {
			_, err := stmt.ExecContext(ctx,
				batchData.DeviceIDs[j],
				batchData.Timestamps[j],
				batchData.Latitudes[j],
				batchData.Longitudes[j],
				batchData.LoadDate)
			if err != nil {
				return totalInserted, fmt.Errorf("failed to execute batch insert: %w", err)
			}
		}
		totalInserted += int64(end - i)
	}

	if err := tx.Commit(); err != nil {
		return totalInserted, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return totalInserted, nil
}

func (loader *ParquetLoader) extractTableData(table arrow.Table, loadDate time.Time) (*BatchInsertData, error) {
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
		var availableColumns []string
		for _, field := range schema.Fields() {
			availableColumns = append(availableColumns, field.Name)
		}
		return nil, fmt.Errorf("required columns not found in %v", availableColumns)
	}

	numRows := int(table.NumRows())
	batchData := &BatchInsertData{
		DeviceIDs:  make([]string, 0, numRows),
		Timestamps: make([]time.Time, 0, numRows),
		Latitudes:  make([]float64, 0, numRows),
		Longitudes: make([]float64, 0, numRows),
		LoadDate:   loadDate,
	}

	deviceIDCol := table.Column(deviceIDIdx)
	timestampCol := table.Column(timestampIdx)
	latCol := table.Column(latIdx)
	lonCol := table.Column(lonIdx)

	for i := 0; i < numRows; i++ {
		deviceID := loader.getStringValue(deviceIDCol, i)
		timestamp := loader.getTimestampValue(timestampCol, i)
		latitude := loader.getFloat64Value(latCol, i)
		longitude := loader.getFloat64Value(lonCol, i)

		// Validation
		if deviceID == "" || timestamp.IsZero() {
			continue
		}

		now := time.Now()
		if timestamp.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) ||
			timestamp.After(now.Add(24*time.Hour)) {
			continue
		}

		if latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
			continue
		}

		batchData.DeviceIDs = append(batchData.DeviceIDs, deviceID)
		batchData.Timestamps = append(batchData.Timestamps, timestamp)
		batchData.Latitudes = append(batchData.Latitudes, latitude)
		batchData.Longitudes = append(batchData.Longitudes, longitude)
	}

	return batchData, nil
}

func (loader *ParquetLoader) insertData(table arrow.Table, loadDate time.Time) (int64, error) {
	conn := loader.getClickHouseConnection()
	defer loader.returnClickHouseConnection(conn)

	ctx, cancel := context.WithTimeout(context.Background(), loader.config.QueryTimeout)
	defer cancel()

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
		var availableColumns []string
		for _, field := range schema.Fields() {
			availableColumns = append(availableColumns, field.Name)
		}
		return 0, fmt.Errorf("required columns not found in %v", availableColumns)
	}

	deviceIDCol := table.Column(deviceIDIdx)
	timestampCol := table.Column(timestampIdx)
	latCol := table.Column(latIdx)
	lonCol := table.Column(lonIdx)

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

			if deviceID == "" || timestamp.IsZero() {
				continue
			}

			now := time.Now()
			if timestamp.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) ||
				timestamp.After(now.Add(24*time.Hour)) {
				continue
			}

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

// Helper functions remain the same
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
		unit := arr.DataType().(*arrow.TimestampType).Unit
		timestamp := arr.Value(row)
		return timestamp.ToTime(unit)
	case *array.Int64:
		epochMillis := arr.Value(row)
		if epochMillis <= 0 {
			return time.Time{}
		}
		return time.Unix(epochMillis/1000, (epochMillis%1000)*1000000).UTC()
	case *array.Uint64:
		epochMillis := int64(arr.Value(row))
		if epochMillis <= 0 {
			return time.Time{}
		}
		return time.Unix(epochMillis/1000, (epochMillis%1000)*1000000).UTC()
	case *array.Int32:
		epochSecs := int64(arr.Value(row))
		if epochSecs <= 0 {
			return time.Time{}
		}
		return time.Unix(epochSecs, 0).UTC()
	case *array.String:
		timeStr := arr.Value(row)
		if timeStr == "" {
			return time.Time{}
		}
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
		return time.Time{}
	default:
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
	log.Printf("🚀 Starting high-speed parallel loading with %d workers...", loader.config.MaxWorkers)

	allFiles, err := loader.getParquetFiles(folders)
	if err != nil {
		return fmt.Errorf("failed to get parquet files: %w", err)
	}

	if len(allFiles) == 0 {
		log.Println("✅ No unprocessed parquet files found!")
		return nil
	}

	jobsChan := make(chan FileJob, len(allFiles))
	resultsChan := make(chan FileResult, len(allFiles))

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

	go func() {
		for _, job := range allFiles {
			jobsChan <- job
		}
		close(jobsChan)
	}()

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	completed := 0
	lastReport := time.Now()

	for result := range resultsChan {
		completed++

		if result.Success {
			loader.stats.IncrementProcessed()
			loader.stats.AddRows(result.RowsProcessed)
		} else {
			loader.stats.AddFailedFile(result.FilePath)
			log.Printf("❌ Failed: %s - %v", filepath.Base(result.FilePath), result.Error)
		}

		// Report progress more frequently for high-speed processing
		if time.Since(lastReport) > 10*time.Second || completed == len(allFiles) {
			rowsPerSecond := float64(atomic.LoadInt64(&loader.stats.TotalRowsInserted)) / time.Since(loader.stats.StartTime).Seconds()
			log.Printf("📈 Progress: %d/%d files | %s rows/sec | %s total rows",
				completed, len(allFiles), formatNumber(int64(rowsPerSecond)), formatNumber(atomic.LoadInt64(&loader.stats.TotalRowsInserted)))
			lastReport = time.Now()
		}
	}

	loader.printStats()
	return nil
}

func (loader *ParquetLoader) printStats() {
	duration := time.Since(loader.stats.StartTime)

	log.Println("=" + strings.Repeat("=", 60))
	log.Println("📊 HIGH-SPEED LOADING COMPLETED")
	log.Println("=" + strings.Repeat("=", 60))
	log.Printf("⏱️  Total time: %v", duration)
	log.Printf("📁 Files processed: %d", atomic.LoadInt64(&loader.stats.TotalFilesProcessed))
	log.Printf("⏭️  Files skipped: %d", len(loader.stats.SkippedFiles))
	log.Printf("📝 Total rows inserted: %s", formatNumber(atomic.LoadInt64(&loader.stats.TotalRowsInserted)))
	log.Printf("❌ Failed files: %d", len(loader.stats.FailedFiles))

	if loader.stats.TotalRowsInserted > 0 && duration.Seconds() > 0 {
		rowsPerSecond := float64(atomic.LoadInt64(&loader.stats.TotalRowsInserted)) / duration.Seconds()
		log.Printf("⚡ Average insertion rate: %s rows/second", formatNumber(int64(rowsPerSecond)))

		// Calculate throughput metrics
		if duration.Minutes() > 0 {
			filesPerMinute := float64(atomic.LoadInt64(&loader.stats.TotalFilesProcessed)) / duration.Minutes()
			log.Printf("📄 Files processed per minute: %.1f", filesPerMinute)
		}
	}

	if len(loader.stats.FailedFiles) > 0 {
		log.Println("\n❌ Failed files:")
		for _, failedFile := range loader.stats.FailedFiles {
			log.Printf("   - %s", filepath.Base(failedFile))
		}
	}
}

func (loader *ParquetLoader) Close() error {
	// Close ClickHouse connections
	close(loader.chConnPool)
	for conn := range loader.chConnPool {
		conn.Close()
	}

	// Close SQLite
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
	// High-performance configuration optimized for maximum speed
	config := LoaderConfig{
		CHHost:         "localhost",
		CHPort:         9000,
		CHDatabase:     "device_tracking",
		CHUser:         "default",
		CHPassword:     "nyros",
		MaxWorkers:     runtime.NumCPU() * 2, // Increased workers
		ChunkSize:      100000,               // Larger chunk size
		QueryTimeout:   10 * time.Minute,     // Increased timeout
		SQLiteDB:       "./parquet_loader_tracking.db",
		BatchInserts:   true,             // Enable batch inserts
		ConnectionPool: runtime.NumCPU(), // Connection pool size
		RetryAttempts:  3,                // Retry failed operations
	}

	log.Printf("🔧 High-Performance Configuration:")
	log.Printf("   Workers: %d", config.MaxWorkers)
	log.Printf("   Chunk Size: %s", formatNumber(int64(config.ChunkSize)))
	log.Printf("   Connection Pool: %d", config.ConnectionPool)
	log.Printf("   Batch Inserts: %v", config.BatchInserts)
	log.Printf("   Retry Attempts: %d", config.RetryAttempts)

	startDateStr := "2025-09-15"
	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		panic(err)
	}

	n := 8

	var targetDates []string
	for i := 0; i < n; i++ {
		date := startDate.AddDate(0, 0, -i)
		targetDates = append(targetDates, date.Format("20060102"))
	}

	parquetFolders := []string{}
	for _, date := range targetDates {
		parquetFolders = append(parquetFolders, fmt.Sprintf("/mnt/blobcontainer/load_date=%s", date))
	}

	loader := NewParquetLoader(config)
	defer loader.Close()

	// Initialize SQLite tracking with optimizations
	if err := loader.initSQLiteTracking(); err != nil {
		log.Fatalf("❌ SQLite tracking setup failed: %v", err)
	}

	// Initialize ClickHouse connection pool
	if err := loader.initClickHousePool(); err != nil {
		log.Fatalf("❌ ClickHouse connection pool setup failed: %v", err)
	}

	// Setup ClickHouse database
	if err := loader.setupClickHouse(); err != nil {
		log.Fatalf("❌ ClickHouse setup failed: %v", err)
	}

	// Pre-warm the system
	log.Println("🔥 Pre-warming system for maximum performance...")
	runtime.GC() // Force garbage collection before starting

	// Load files with high-speed parallel processing
	startTime := time.Now()
	if err := loader.loadFiles(parquetFolders); err != nil {
		log.Fatalf("❌ Loading failed: %v", err)
	}

	totalTime := time.Since(startTime)
	log.Printf("🎉 High-speed parallel loading completed in %v!", totalTime)
	log.Println("✨ System optimized for maximum throughput and efficiency!")
}
