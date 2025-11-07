package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

const (
	MaxFileSizeMB = 100 // Target max size for each parquet file
	MaxFileSize   = MaxFileSizeMB * 1024 * 1024
)

var NumWorkers = runtime.NumCPU() * 3

type ConsumerRecord struct {
	ID              string
	Latitude        float64
	Longitude       float64
	PersonFirstName string
	PersonLastName  string
	PrimaryAddress  string
	TenDigitPhone   string
	Email           string
	CityName        string
	State           string
	ZipCode         string
}

type SimpleConverter struct {
	csvPath       string
	outputFolder  string
	logger        *log.Logger
	schema        *arrow.Schema
	fileCounter   int32
	totalRecords  atomic.Int64
	skippedRows   atomic.Int64
	startTime     time.Time
	currentWriter *pqarrow.FileWriter
	currentFile   *os.File
	currentSize   int64
	recordBuffer  []ConsumerRecord
	mutex         sync.Mutex
	writerMutex   sync.Mutex
	recordsInFile int64
}

func NewSimpleConverter(csvPath, outputFolder string) (*SimpleConverter, error) {
	logger := log.New(os.Stdout, "", 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "latitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "longitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "PersonFirstName", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "PersonLastName", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "PrimaryAddress", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "TenDigitPhone", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "Email", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "CityName", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "State", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "ZipCode", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	if err := os.MkdirAll(outputFolder, 0755); err != nil {
		return nil, err
	}

	return &SimpleConverter{
		csvPath:      csvPath,
		outputFolder: outputFolder,
		logger:       logger,
		schema:       schema,
		recordBuffer: make([]ConsumerRecord, 0),
	}, nil
}

func (sc *SimpleConverter) createNewWriter() error {
	fileName := fmt.Sprintf("consumers_chunk_%04d.parquet", atomic.LoadInt32(&sc.fileCounter))
	filePath := filepath.Join(sc.outputFolder, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)

	writer, err := pqarrow.NewFileWriter(sc.schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create writer: %w", err)
	}

	sc.currentWriter = writer
	sc.currentFile = file
	sc.currentSize = 0
	sc.recordsInFile = 0
	atomic.AddInt32(&sc.fileCounter, 1)

	sc.logger.Printf("📝 Created new file: %s", fileName)

	return nil
}

func (sc *SimpleConverter) closeCurrentWriter() error {
	if sc.currentWriter == nil {
		return nil
	}

	// Flush any remaining records
	if len(sc.recordBuffer) > 0 {
		if err := sc.writeBufferedRecords(); err != nil {
			return err
		}
	}

	if err := sc.currentWriter.Close(); err != nil {
		return err
	}

	filePath := sc.currentFile.Name()

	if err := sc.currentFile.Close(); err != nil {
		return err
	}

	// Get final file size
	info, err := os.Stat(filePath)
	if err == nil {
		sc.logger.Printf("✅ %s → %d records (%s)",
			filepath.Base(filePath), sc.recordsInFile, formatSize(info.Size()))
	}

	sc.currentWriter = nil
	sc.currentFile = nil
	sc.recordsInFile = 0

	return nil
}

func (sc *SimpleConverter) writeBufferedRecords() error {
	if len(sc.recordBuffer) == 0 {
		return nil
	}

	pool := memory.NewGoAllocator()

	// Create builders
	idBuilder := array.NewStringBuilder(pool)
	latBuilder := array.NewFloat64Builder(pool)
	lonBuilder := array.NewFloat64Builder(pool)
	firstNameBuilder := array.NewStringBuilder(pool)
	lastNameBuilder := array.NewStringBuilder(pool)
	addressBuilder := array.NewStringBuilder(pool)
	phoneBuilder := array.NewStringBuilder(pool)
	emailBuilder := array.NewStringBuilder(pool)
	cityBuilder := array.NewStringBuilder(pool)
	stateBuilder := array.NewStringBuilder(pool)
	zipBuilder := array.NewStringBuilder(pool)

	// Append all records
	for i := range sc.recordBuffer {
		rec := &sc.recordBuffer[i]
		idBuilder.Append(rec.ID)
		latBuilder.Append(rec.Latitude)
		lonBuilder.Append(rec.Longitude)
		firstNameBuilder.Append(rec.PersonFirstName)
		lastNameBuilder.Append(rec.PersonLastName)
		addressBuilder.Append(rec.PrimaryAddress)
		phoneBuilder.Append(rec.TenDigitPhone)
		emailBuilder.Append(rec.Email)
		cityBuilder.Append(rec.CityName)
		stateBuilder.Append(rec.State)
		zipBuilder.Append(rec.ZipCode)
	}

	// Build arrays
	idArray := idBuilder.NewArray()
	latArray := latBuilder.NewArray()
	lonArray := lonBuilder.NewArray()
	firstNameArray := firstNameBuilder.NewArray()
	lastNameArray := lastNameBuilder.NewArray()
	addressArray := addressBuilder.NewArray()
	phoneArray := phoneBuilder.NewArray()
	emailArray := emailBuilder.NewArray()
	cityArray := cityBuilder.NewArray()
	stateArray := stateBuilder.NewArray()
	zipArray := zipBuilder.NewArray()

	defer idArray.Release()
	defer latArray.Release()
	defer lonArray.Release()
	defer firstNameArray.Release()
	defer lastNameArray.Release()
	defer addressArray.Release()
	defer phoneArray.Release()
	defer emailArray.Release()
	defer cityArray.Release()
	defer stateArray.Release()
	defer zipArray.Release()

	// Create record
	record := array.NewRecord(
		sc.schema,
		[]arrow.Array{idArray, latArray, lonArray, firstNameArray, lastNameArray,
			addressArray, phoneArray, emailArray, cityArray, stateArray, zipArray},
		int64(len(sc.recordBuffer)),
	)
	defer record.Release()

	// Write to file
	if err := sc.currentWriter.Write(record); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	// Track records added to this file
	sc.recordsInFile += int64(len(sc.recordBuffer))

	// Clear buffer
	sc.recordBuffer = sc.recordBuffer[:0]

	return nil
}

func (sc *SimpleConverter) checkFileSize() (bool, error) {
	if sc.currentFile == nil {
		return false, nil
	}

	// Get current file size
	info, err := os.Stat(sc.currentFile.Name())
	if err != nil {
		return false, err
	}

	sc.currentSize = info.Size()

	// If file exceeds max size, close and return true
	if sc.currentSize >= MaxFileSize {
		if err := sc.closeCurrentWriter(); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (sc *SimpleConverter) addRecord(record ConsumerRecord) error {
	sc.writerMutex.Lock()
	defer sc.writerMutex.Unlock()

	// Create first writer if needed
	if sc.currentWriter == nil {
		if err := sc.createNewWriter(); err != nil {
			return err
		}
	}

	// Add to buffer
	sc.recordBuffer = append(sc.recordBuffer, record)

	// Write buffer periodically (every 10000 records) to check file size
	if len(sc.recordBuffer) >= 10000 {
		if err := sc.writeBufferedRecords(); err != nil {
			return err
		}

		// Check if file size exceeded
		needNewFile, err := sc.checkFileSize()
		if err != nil {
			return err
		}

		// If we need a new file, create it
		if needNewFile {
			if err := sc.createNewWriter(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sc *SimpleConverter) Convert() error {
	sc.logger.Println("╔═══════════════════════════════════════════════════════╗")
	sc.logger.Println("║  CSV TO PARQUET - PARALLEL CONVERTER                 ║")
	sc.logger.Println("╚═══════════════════════════════════════════════════════╝")
	sc.logger.Printf("Input:         %s", sc.csvPath)
	sc.logger.Printf("Output:        %s", sc.outputFolder)
	sc.logger.Printf("Max file size: %d MB", MaxFileSizeMB)
	sc.logger.Printf("Workers:       %d (CPU cores: %d)\n", NumWorkers, runtime.NumCPU())

	file, err := os.Open(sc.csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Build column index
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[col] = i
	}

	sc.logger.Printf("CSV Columns: %d", len(header))

	// Verify required columns
	requiredCols := []string{"IndividualID", "Latitude", "Longitude"}
	for _, col := range requiredCols {
		if _, ok := colIndex[col]; !ok {
			return fmt.Errorf("missing required column: %s", col)
		}
	}

	sc.logger.Println("\n📦 Processing records...")
	sc.startTime = time.Now()

	// Channel for raw CSV rows
	rowChan := make(chan []string, NumWorkers*1000)
	// Channel for parsed records
	recordChan := make(chan ConsumerRecord, NumWorkers*1000)

	var wg sync.WaitGroup

	// Start parser workers
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for row := range rowChan {
				record, err := sc.parseRecord(row, colIndex)
				if err != nil {
					sc.skippedRows.Add(1)
					continue
				}
				recordChan <- record
			}
		}(i)
	}

	// Start writer goroutine
	writerDone := make(chan error, 1)
	go func() {
		for record := range recordChan {
			if err := sc.addRecord(record); err != nil {
				sc.logger.Printf("Error adding record: %v", err)
				continue
			}
			sc.totalRecords.Add(1)
		}
		writerDone <- nil
	}()

	// Progress reporter
	stopProgress := make(chan bool)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(sc.startTime)
				rate := float64(sc.totalRecords.Load()) / elapsed.Seconds()
				sc.logger.Printf("  ⚡ %.0f records/sec", rate)
			case <-stopProgress:
				return
			}
		}
	}()

	// Read CSV and send to workers
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			sc.skippedRows.Add(1)
			continue
		}

		// Make a copy of the row since reader reuses it
		rowCopy := make([]string, len(row))
		copy(rowCopy, row)
		rowChan <- rowCopy
	}

	// Shutdown sequence
	close(rowChan)       // No more rows
	wg.Wait()            // Wait for all parsers
	close(recordChan)    // No more records
	<-writerDone         // Wait for writer
	stopProgress <- true // Stop progress reporter

	// Close final writer
	sc.writerMutex.Lock()
	if err := sc.closeCurrentWriter(); err != nil {
		sc.writerMutex.Unlock()
		return err
	}
	sc.writerMutex.Unlock()

	elapsed := time.Since(sc.startTime)
	avgRate := float64(sc.totalRecords.Load()) / elapsed.Seconds()

	sc.logger.Println("\n╔═══════════════════════════════════════════════════════╗")
	sc.logger.Println("║  ✅ CONVERSION COMPLETE                               ║")
	sc.logger.Println("╚═══════════════════════════════════════════════════════╝")
	sc.logger.Printf("Total records:   %d", sc.totalRecords.Load())
	sc.logger.Printf("Skipped rows:    %d", sc.skippedRows.Load())
	sc.logger.Printf("Output files:    %d", atomic.LoadInt32(&sc.fileCounter))
	sc.logger.Printf("Time elapsed:    %v", elapsed)
	sc.logger.Printf("Average rate:    %.0f records/sec", avgRate)
	sc.logger.Printf("Output location: %s\n", sc.outputFolder)

	return nil
}

func (sc *SimpleConverter) parseRecord(row []string, colIndex map[string]int) (ConsumerRecord, error) {
	var record ConsumerRecord

	// Get ID
	if idx, ok := colIndex["IndividualID"]; ok && idx < len(row) {
		record.ID = row[idx]
		if record.ID == "" {
			return record, fmt.Errorf("empty ID")
		}
	} else {
		return record, fmt.Errorf("missing ID")
	}

	// Parse latitude
	if idx, ok := colIndex["Latitude"]; ok && idx < len(row) {
		lat, err := strconv.ParseFloat(row[idx], 64)
		if err != nil || lat < -90 || lat > 90 {
			return record, fmt.Errorf("invalid latitude")
		}
		record.Latitude = lat
	} else {
		return record, fmt.Errorf("missing latitude")
	}

	// Parse longitude
	if idx, ok := colIndex["Longitude"]; ok && idx < len(row) {
		lon, err := strconv.ParseFloat(row[idx], 64)
		if err != nil || lon < -180 || lon > 180 {
			return record, fmt.Errorf("invalid longitude")
		}
		record.Longitude = lon
	} else {
		return record, fmt.Errorf("missing longitude")
	}

	// Parse optional string fields
	if idx, ok := colIndex["PersonFirstName"]; ok && idx < len(row) {
		record.PersonFirstName = row[idx]
	}
	if idx, ok := colIndex["PersonLastName"]; ok && idx < len(row) {
		record.PersonLastName = row[idx]
	}
	if idx, ok := colIndex["PrimaryAddress"]; ok && idx < len(row) {
		record.PrimaryAddress = row[idx]
	}
	if idx, ok := colIndex["TenDigitPhone"]; ok && idx < len(row) {
		record.TenDigitPhone = row[idx]
	}
	if idx, ok := colIndex["Email"]; ok && idx < len(row) {
		record.Email = row[idx]
	}
	if idx, ok := colIndex["CityName"]; ok && idx < len(row) {
		record.CityName = row[idx]
	}
	if idx, ok := colIndex["State"]; ok && idx < len(row) {
		record.State = row[idx]
	}
	if idx, ok := colIndex["ZipCode"]; ok && idx < len(row) {
		record.ZipCode = row[idx]
	}

	return record, nil
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	csvPath := "/root/device_tracker/data/ConsumerData.csv"
	outputFolder := "/home/device-tracker/data/output/consumers"

	if len(os.Args) > 1 {
		csvPath = os.Args[1]
	}
	if len(os.Args) > 2 {
		outputFolder = os.Args[2]
	}

	converter, err := NewSimpleConverter(csvPath, outputFolder)
	if err != nil {
		log.Fatalf("Failed to create converter: %v", err)
	}

	if err := converter.Convert(); err != nil {
		log.Fatalf("Conversion failed: %v", err)
	}
}
