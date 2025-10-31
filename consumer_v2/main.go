package main

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
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
	MaxOutputFiles  = 500
	RecordsPerBatch = 700000
	BufferedRecords = 1000000
)

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

type ParquetWriter struct {
	mutex      sync.Mutex
	batch      []ConsumerRecord
	writer     *pqarrow.FileWriter
	file       *os.File
	schema     *arrow.Schema
	chunkID    int
	totalCount int64
}

type StreamConverter struct {
	csvPath      string
	outputFolder string
	logger       *log.Logger
	writers      []*ParquetWriter
	totalRecords atomic.Int64
	skippedRows  atomic.Int64
	startTime    time.Time
}

func NewStreamConverter(csvPath, outputFolder string) (*StreamConverter, error) {
	logger := log.New(os.Stdout, "", 0)

	return &StreamConverter{
		csvPath:      csvPath,
		outputFolder: outputFolder,
		logger:       logger,
	}, nil
}

func (sc *StreamConverter) initWriters() error {
	if err := os.MkdirAll(sc.outputFolder, 0755); err != nil {
		return err
	}

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

	sc.writers = make([]*ParquetWriter, MaxOutputFiles)

	sc.logger.Printf("Initializing %d parquet writers...", MaxOutputFiles)

	for i := 0; i < MaxOutputFiles; i++ {
		chunkPath := filepath.Join(sc.outputFolder, fmt.Sprintf("consumers_chunk_%03d.parquet", i))

		file, err := os.Create(chunkPath)
		if err != nil {
			return fmt.Errorf("failed to create chunk %d: %w", i, err)
		}

		props := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
			parquet.WithDictionaryDefault(true),
		)

		writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to create writer %d: %w", i, err)
		}

		sc.writers[i] = &ParquetWriter{
			batch:   make([]ConsumerRecord, 0, RecordsPerBatch),
			writer:  writer,
			file:    file,
			schema:  schema,
			chunkID: i,
		}
	}

	sc.logger.Println("✅ Writers initialized")
	return nil
}

func (sc *StreamConverter) hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (sc *StreamConverter) addRecord(record ConsumerRecord) error {
	// Distribute by hash of ID
	chunkIdx := int(sc.hashString(record.ID)) % MaxOutputFiles
	writer := sc.writers[chunkIdx]

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	writer.batch = append(writer.batch, record)
	writer.totalCount++

	if len(writer.batch) >= RecordsPerBatch {
		return sc.flushWriter(writer)
	}

	return nil
}

func (sc *StreamConverter) flushWriter(w *ParquetWriter) error {
	if len(w.batch) == 0 {
		return nil
	}

	pool := memory.NewGoAllocator()

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

	for i := range w.batch {
		rec := &w.batch[i]
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

	record := array.NewRecord(
		w.schema,
		[]arrow.Array{idArray, latArray, lonArray, firstNameArray, lastNameArray,
			addressArray, phoneArray, emailArray, cityArray, stateArray, zipArray},
		int64(len(w.batch)),
	)
	defer record.Release()

	if err := w.writer.Write(record); err != nil {
		return fmt.Errorf("chunk %d write error: %w", w.chunkID, err)
	}

	w.batch = w.batch[:0]
	return nil
}

func (sc *StreamConverter) closeAllWriters() error {
	sc.logger.Println("\nFlushing remaining batches...")

	nonEmptyChunks := 0
	for _, w := range sc.writers {
		if w != nil {
			w.mutex.Lock()
			if err := sc.flushWriter(w); err != nil {
				sc.logger.Printf("Error flushing chunk %d: %v", w.chunkID, err)
			}
			if w.totalCount > 0 {
				nonEmptyChunks++
			}
			w.writer.Close()
			w.file.Close()
			w.mutex.Unlock()
		}
	}

	sc.logger.Printf("✅ Closed %d writers (%d non-empty)", MaxOutputFiles, nonEmptyChunks)
	return nil
}

func (sc *StreamConverter) Convert() error {
	sc.logger.Println("╔═══════════════════════════════════════════════════════╗")
	sc.logger.Println("║  CSV TO PARQUET - STREAMING CONVERTER                ║")
	sc.logger.Println("╚═══════════════════════════════════════════════════════╝")
	sc.logger.Printf("Input:  %s", sc.csvPath)
	sc.logger.Printf("Output: %s", sc.outputFolder)
	sc.logger.Printf("Chunks: %d\n", MaxOutputFiles)

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

	// Verify required columns exist
	requiredCols := []string{"IndividualID", "Latitude", "Longitude"}
	for _, col := range requiredCols {
		if _, ok := colIndex[col]; !ok {
			return fmt.Errorf("missing required column: %s", col)
		}
	}

	// Initialize writers
	if err := sc.initWriters(); err != nil {
		return err
	}

	sc.logger.Println("\n📦 Processing records (streaming mode)...")
	sc.startTime = time.Now()

	lineNum := 1
	lastUpdate := time.Now()

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			sc.skippedRows.Add(1)
			lineNum++
			continue
		}

		lineNum++

		record, err := sc.parseRecord(row, colIndex)
		if err != nil {
			sc.skippedRows.Add(1)
			continue
		}

		if err := sc.addRecord(record); err != nil {
			sc.logger.Printf("Error adding record: %v", err)
			continue
		}

		sc.totalRecords.Add(1)

		// Progress update every 5 seconds
		if time.Since(lastUpdate) >= 5*time.Second {
			elapsed := time.Since(sc.startTime)
			rate := float64(sc.totalRecords.Load()) / elapsed.Seconds()
			sc.logger.Printf("  ⚡ %d records (%.0f rec/sec, %.0f min elapsed)",
				sc.totalRecords.Load(), rate, elapsed.Minutes())
			lastUpdate = time.Now()
			runtime.GC()
		}
	}

	if err := sc.closeAllWriters(); err != nil {
		return err
	}

	elapsed := time.Since(sc.startTime)
	avgRate := float64(sc.totalRecords.Load()) / elapsed.Seconds()

	sc.logger.Println("\n╔═══════════════════════════════════════════════════════╗")
	sc.logger.Println("║  ✅ CONVERSION COMPLETE                               ║")
	sc.logger.Println("╚═══════════════════════════════════════════════════════╝")
	sc.logger.Printf("Total records:   %d", sc.totalRecords.Load())
	sc.logger.Printf("Skipped rows:    %d", sc.skippedRows.Load())
	sc.logger.Printf("Time elapsed:    %v", elapsed)
	sc.logger.Printf("Average rate:    %.0f records/sec", avgRate)
	sc.logger.Printf("Output location: %s\n", sc.outputFolder)

	// Show file stats
	sc.printFileStats()

	return nil
}

func (sc *StreamConverter) parseRecord(row []string, colIndex map[string]int) (ConsumerRecord, error) {
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

func (sc *StreamConverter) printFileStats() {
	sc.logger.Println("📁 Output files:")

	nonEmpty := 0
	var totalSize int64

	for _, w := range sc.writers {
		if w.totalCount > 0 {
			nonEmpty++
			if nonEmpty <= 5 {
				path := filepath.Join(sc.outputFolder, fmt.Sprintf("consumers_chunk_%03d.parquet", w.chunkID))
				info, err := os.Stat(path)
				if err == nil {
					sc.logger.Printf("   Chunk %03d: %7d records (%s)",
						w.chunkID, w.totalCount, formatSize(info.Size()))
					totalSize += info.Size()
				}
			}
		}
	}

	if nonEmpty > 5 {
		sc.logger.Printf("   ... and %d more chunks\n", nonEmpty-5)
	}

	sc.logger.Printf("Total chunks: %d (non-empty)", nonEmpty)
	sc.logger.Printf("Total size:   %s\n", formatSize(totalSize))
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

	converter, err := NewStreamConverter(csvPath, outputFolder)
	if err != nil {
		log.Fatalf("Failed to create converter: %v", err)
	}

	if err := converter.Convert(); err != nil {
		log.Fatalf("Conversion failed: %v", err)
	}
}
