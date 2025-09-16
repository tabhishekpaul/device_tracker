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
	"strconv"
	"time"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/mattn/go-sqlite3"
)

type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type ProcessedFile struct {
	FilePath    string
	FileHash    string
	ProcessedAt time.Time
	RecordCount int
}

type ConsumerParquetLoader struct {
	chConn  driver.Conn
	localDB *sql.DB
	logger  *log.Logger
	dbPath  string
}

func NewConsumerParquetLoader(chConfig ClickHouseConfig, localDBPath string) (*ConsumerParquetLoader, error) {
	logger := log.New(os.Stdout, "[ConsumerParquetLoader] ", log.LstdFlags|log.Lmicroseconds)

	// Initialize ClickHouse connection
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", chConfig.Host, chConfig.Port)},
		Auth: clickhouse.Auth{
			Database: chConfig.Database,
			Username: chConfig.Username,
			Password: chConfig.Password,
		},
		DialTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := chConn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// Initialize local SQLite database
	localDB, err := sql.Open("sqlite3", localDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local database: %w", err)
	}

	loader := &ConsumerParquetLoader{
		chConn:  chConn,
		localDB: localDB,
		logger:  logger,
		dbPath:  localDBPath,
	}

	if err := loader.initLocalDB(); err != nil {
		return nil, fmt.Errorf("failed to initialize local database: %w", err)
	}

	return loader, nil
}

func (cpl *ConsumerParquetLoader) initLocalDB() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS processed_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_path TEXT NOT NULL UNIQUE,
		file_hash TEXT NOT NULL,
		processed_at DATETIME NOT NULL,
		record_count INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path);
	CREATE INDEX IF NOT EXISTS idx_file_hash ON processed_files(file_hash);
	`

	if _, err := cpl.localDB.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	cpl.logger.Printf("Local tracking database initialized at: %s", cpl.dbPath)
	return nil
}

func (cpl *ConsumerParquetLoader) calculateFileHash(filePath string) (string, error) {
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

func (cpl *ConsumerParquetLoader) isFileProcessed(filePath string) (bool, string, error) {
	// Calculate current file hash
	currentHash, err := cpl.calculateFileHash(filePath)
	if err != nil {
		return false, "", fmt.Errorf("failed to calculate file hash: %w", err)
	}

	// Check if file with same path and hash exists
	var existingHash string
	var processedAt string
	query := "SELECT file_hash, processed_at FROM processed_files WHERE file_path = ?"

	err = cpl.localDB.QueryRow(query, filePath).Scan(&existingHash, &processedAt)
	if err == sql.ErrNoRows {
		return false, currentHash, nil // File not processed
	}
	if err != nil {
		return false, "", fmt.Errorf("failed to check processed files: %w", err)
	}

	// If hashes match, file was already processed
	if existingHash == currentHash {
		cpl.logger.Printf("File %s already processed at %s (hash: %s)",
			filepath.Base(filePath), processedAt, currentHash[:8])
		return true, currentHash, nil
	}

	// File exists but hash is different (file was modified)
	cpl.logger.Printf("File %s was modified since last processing, will reprocess", filepath.Base(filePath))
	return false, currentHash, nil
}

func (cpl *ConsumerParquetLoader) markFileAsProcessed(filePath, fileHash string, recordCount int) error {
	// Use REPLACE to handle both insert and update cases
	query := `
	REPLACE INTO processed_files (file_path, file_hash, processed_at, record_count)
	VALUES (?, ?, ?, ?)
	`

	_, err := cpl.localDB.Exec(query, filePath, fileHash, time.Now(), recordCount)
	if err != nil {
		return fmt.Errorf("failed to mark file as processed: %w", err)
	}

	return nil
}

func (cpl *ConsumerParquetLoader) getProcessedFilesStats() (int, int, error) {
	var totalFiles, totalRecords int

	query := "SELECT COUNT(*), COALESCE(SUM(record_count), 0) FROM processed_files"
	err := cpl.localDB.QueryRow(query).Scan(&totalFiles, &totalRecords)

	return totalFiles, totalRecords, err
}

func (cpl *ConsumerParquetLoader) LoadConsumersFromParquet(ctx context.Context, parquetFolder string) error {
	cpl.logger.Printf("Loading consumers from parquet files in %s", parquetFolder)

	files, err := filepath.Glob(filepath.Join(parquetFolder, "*.parquet"))
	if err != nil {
		return fmt.Errorf("failed to list parquet files: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no parquet files found in %s", parquetFolder)
	}

	// Get initial stats
	processedFiles, processedRecords, _ := cpl.getProcessedFilesStats()
	cpl.logger.Printf("Found %d parquet files. Previously processed: %d files, %d records",
		len(files), processedFiles, processedRecords)

	totalRecords := 0
	skippedFiles := 0
	processedFilesCount := 0
	batchSize := 100000

	for idx, filePath := range files {
		cpl.logger.Printf("Checking file %d/%d: %s", idx+1, len(files), filepath.Base(filePath))

		// Check if file was already processed
		isProcessed, fileHash, err := cpl.isFileProcessed(filePath)
		if err != nil {
			cpl.logger.Printf("Error checking file status: %v", err)
			continue
		}

		if isProcessed {
			skippedFiles++
			continue
		}

		// Process the file
		fileRecords, err := cpl.processParquetFile(ctx, filePath, batchSize)
		if err != nil {
			cpl.logger.Printf("Error processing file %s: %v", filepath.Base(filePath), err)
			continue
		}

		// Mark file as processed
		if err := cpl.markFileAsProcessed(filePath, fileHash, fileRecords); err != nil {
			cpl.logger.Printf("Warning: failed to mark file as processed: %v", err)
		}

		totalRecords += fileRecords
		processedFilesCount++

		cpl.logger.Printf("✅ Processed file %s: %d records", filepath.Base(filePath), fileRecords)
	}

	cpl.logger.Printf("🎉 Processing complete!")
	cpl.logger.Printf("   - Files processed: %d", processedFilesCount)
	cpl.logger.Printf("   - Files skipped: %d", skippedFiles)
	cpl.logger.Printf("   - New records loaded: %d", totalRecords)

	// Final stats
	finalFiles, finalRecords, _ := cpl.getProcessedFilesStats()
	cpl.logger.Printf("   - Total files in tracking DB: %d", finalFiles)
	cpl.logger.Printf("   - Total records in tracking DB: %d", finalRecords)

	return nil
}

func (cpl *ConsumerParquetLoader) processParquetFile(ctx context.Context, filePath string, batchSize int) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.NewGoAllocator())
	if err != nil {
		return 0, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	recordReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get record reader: %w", err)
	}
	defer recordReader.Release()

	batch, err := cpl.chConn.PrepareBatch(ctx, `INSERT INTO consumers 
		(id, latitude, longitude, PersonFirstName, PersonLastName, PrimaryAddress, TenDigitPhone, Email, CityName, State, ZipCode)`)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare batch: %w", err)
	}

	fileRecords := 0

	for recordReader.Next() {
		rec := recordReader.Record()
		rows := int(rec.NumRows())
		fields := rec.Schema().Fields()

		colIndex := make(map[string]int)
		for i, f := range fields {
			colIndex[f.Name] = i
		}

		for i := 0; i < rows; i++ {
			id := uint64(0)
			lat := float64(0)
			long := float64(0)
			firstName, lastName, address, phone, email, city, state, zip := "", "", "", "", "", "", "", ""

			if idx, ok := colIndex["id"]; ok {
				if col, ok := rec.Column(idx).(*array.Int64); ok {
					id = uint64(col.Value(i))
				}
			}

			if idx, ok := colIndex["latitude"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Float64:
					lat = col.Value(i)
				case *array.String:
					val := col.Value(i)
					if f, err := strconv.ParseFloat(val, 64); err == nil {
						lat = f
					}
				}
			}

			if idx, ok := colIndex["longitude"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Float64:
					long = col.Value(i)
				case *array.String:
					val := col.Value(i)
					if f, err := strconv.ParseFloat(val, 64); err == nil {
						long = f
					}
				}
			}

			if lat < -90 || lat > 90 || long < -180 || long > 180 {
				continue
			}

			if idx, ok := colIndex["PersonFirstName"]; ok {
				firstName = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["PersonLastName"]; ok {
				lastName = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["PrimaryAddress"]; ok {
				address = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["TenDigitPhone"]; ok {
				phone = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["Email"]; ok {
				email = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["CityName"]; ok {
				city = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["State"]; ok {
				state = rec.Column(idx).(*array.String).Value(i)
			}
			if idx, ok := colIndex["ZipCode"]; ok {
				zip = rec.Column(idx).(*array.String).Value(i)
			}

			if err := batch.Append(id, lat, long, firstName, lastName, address, phone, email, city, state, zip); err != nil {
				cpl.logger.Printf("Error appending: %v", err)
				continue
			}

			fileRecords++
			if fileRecords%batchSize == 0 {
				if err := batch.Send(); err != nil {
					return 0, fmt.Errorf("batch send error: %w", err)
				}
				batch, _ = cpl.chConn.PrepareBatch(ctx, `INSERT INTO consumers 
					(id, latitude, longitude, PersonFirstName, PersonLastName, PrimaryAddress, TenDigitPhone, Email, CityName, State, ZipCode)`)
			}
		}
		rec.Release()
	}

	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("failed to send final batch: %w", err)
	}

	runtime.GC()
	return fileRecords, nil
}

func (cpl *ConsumerParquetLoader) Close() {
	if cpl.chConn != nil {
		cpl.chConn.Close()
	}
	if cpl.localDB != nil {
		cpl.localDB.Close()
	}
}

func main() {
	chConfig := ClickHouseConfig{
		Host:     "172.173.97.164",
		Port:     9000,
		Database: "device_tracking",
		Username: "default",
		Password: "nyros",
	}

	// Local SQLite database path
	localDBPath := "./parquet_tracking.db"

	loader, err := NewConsumerParquetLoader(chConfig, localDBPath)
	if err != nil {
		log.Fatalf("Failed to create loader: %v", err)
	}
	defer loader.Close()

	ctx := context.Background()
	parquetPath := "/home/device-tracker/data/output/consumer_raw/"
	if err := loader.LoadConsumersFromParquet(ctx, parquetPath); err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}
}
