package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

const (
	csvBatchSize        = 10000
	workerPoolSize      = 8
	recordBufferSize    = 100000
	clickhouseBatchSize = 25000
)

type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type DeviceTracker struct {
	ctx           context.Context
	FilterInTime  string
	FilterOutTime string

	TimeColumnName string
	DeviceIDColumn string
	LatColumn      string
	LonColumn      string

	LocFolder    string
	LocData      []LocationRecord
	LocDataMutex sync.RWMutex

	spatialIndex map[int][]int

	OutputFolder string

	OutCampaignDevices string
	OutTargetDevices   string

	IdleDeviceBuffer float64
	NumWorkers       int

	// ClickHouse connection
	clickhouseConn *sql.DB
	chMutex        sync.Mutex
	chBatch        []TimeFilteredRecord
	targetDates    []TimeFilterPeriod

	// Device ID filter for time filtering
	deviceIDFilter map[string]struct{}
	filterMutex    sync.RWMutex
}

type LocationRecord struct {
	Address  string
	Campaign string
	Geometry orb.Polygon
	Bounds   orb.Bound
}

type DeviceRecord struct {
	DeviceID       string
	EventTimestamp time.Time
	Latitude       float64
	Longitude      float64
	InsertDate     time.Time
	Address        string
	Campaign       string
}

type TimeFilteredRecord struct {
	DeviceID       string
	EventTimestamp time.Time
	Latitude       float64
	Longitude      float64
	LoadDate       time.Time
}

type TimeFilterPeriod struct {
	StartTime time.Time
	EndTime   time.Time
}

func NewDeviceTracker(locFolder, outputFolder string, chConfig ClickHouseConfig) (*DeviceTracker, error) {
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	// Connect to ClickHouse
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		chConfig.Username,
		chConfig.Password,
		chConfig.Host,
		chConfig.Port,
		chConfig.Database,
	)

	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	fmt.Println("Successfully connected to ClickHouse")

	return &DeviceTracker{
		ctx:                context.Background(),
		FilterInTime:       "02:00:00",
		FilterOutTime:      "04:30:00",
		TimeColumnName:     "event_timestamp",
		DeviceIDColumn:     "device_id",
		LatColumn:          "latitude",
		LonColumn:          "longitude",
		LocFolder:          locFolder,
		OutputFolder:       outputFolder,
		OutCampaignDevices: "Devices_Within_Campaign.csv",
		OutTargetDevices:   "Target_Idle_Devices",
		IdleDeviceBuffer:   10.0,
		NumWorkers:         numWorkers,
		spatialIndex:       make(map[int][]int),
		clickhouseConn:     conn,
		chBatch:            make([]TimeFilteredRecord, 0, clickhouseBatchSize),
		deviceIDFilter:     make(map[string]struct{}),
	}, nil
}

func (dt *DeviceTracker) Close() error {
	// Flush any remaining records
	dt.chMutex.Lock()
	defer dt.chMutex.Unlock()

	if len(dt.chBatch) > 0 {
		if err := dt.flushClickHouseBatchUnsafe(); err != nil {
			fmt.Printf("Error flushing final batch: %v\n", err)
		}
	}

	if dt.clickhouseConn != nil {
		return dt.clickhouseConn.Close()
	}
	return nil
}

func (dt *DeviceTracker) setTargetDates(dates []string) error {
	dt.targetDates = make([]TimeFilterPeriod, 0, len(dates))

	for _, dateStr := range dates {
		startTime, err := time.Parse("2006-01-02 15:04:05", dateStr+" "+dt.FilterInTime)
		if err != nil {
			return fmt.Errorf("failed to parse start time for %s: %w", dateStr, err)
		}

		endTime, err := time.Parse("2006-01-02 15:04:05", dateStr+" "+dt.FilterOutTime)
		if err != nil {
			return fmt.Errorf("failed to parse end time for %s: %w", dateStr, err)
		}

		dt.targetDates = append(dt.targetDates, TimeFilterPeriod{
			StartTime: startTime,
			EndTime:   endTime,
		})

		fmt.Printf("Added time filter: %s to %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	}

	return nil
}

func (dt *DeviceTracker) loadDeviceIDFilter() error {
	outCSV := filepath.Join(dt.OutputFolder, dt.OutCampaignDevices)

	records, err := dt.readDeviceCSV(outCSV)
	if err != nil {
		return fmt.Errorf("failed to read campaign devices: %w", err)
	}

	dt.filterMutex.Lock()
	defer dt.filterMutex.Unlock()

	dt.deviceIDFilter = make(map[string]struct{}, len(records))
	for i := range records {
		dt.deviceIDFilter[records[i].DeviceID] = struct{}{}
	}

	fmt.Printf("Loaded %d device IDs for time filtering\n", len(dt.deviceIDFilter))
	return nil
}

func (dt *DeviceTracker) isDeviceIDInFilter(deviceID string) bool {
	dt.filterMutex.RLock()
	defer dt.filterMutex.RUnlock()

	_, exists := dt.deviceIDFilter[deviceID]
	return exists
}

func (dt *DeviceTracker) isWithinTimeFilter(eventTime time.Time) bool {
	for _, period := range dt.targetDates {
		if eventTime.After(period.StartTime) && eventTime.Before(period.EndTime) {
			return true
		}
	}
	return false
}

func (dt *DeviceTracker) addToClickHouseBatch(record TimeFilteredRecord) error {
	dt.chMutex.Lock()
	defer dt.chMutex.Unlock()

	dt.chBatch = append(dt.chBatch, record)

	if len(dt.chBatch) >= clickhouseBatchSize {
		return dt.flushClickHouseBatchUnsafe()
	}

	return nil
}

func (dt *DeviceTracker) flushClickHouseBatchUnsafe() error {
	if len(dt.chBatch) == 0 {
		return nil
	}

	tx, err := dt.clickhouseConn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO time_filtered (
			device_id, 
			event_timestamp, 
			latitude, 
			longitude, 
			load_date
		) VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i := range dt.chBatch {
		_, err := stmt.Exec(
			dt.chBatch[i].DeviceID,
			dt.chBatch[i].EventTimestamp,
			dt.chBatch[i].Latitude,
			dt.chBatch[i].Longitude,
			dt.chBatch[i].LoadDate,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Flushed %d records to ClickHouse\n", len(dt.chBatch))

	// Clear the batch
	dt.chBatch = dt.chBatch[:0]

	return nil
}

func (dt *DeviceTracker) getSpatialKey(lon, lat float64) int {
	gridSize := 0.01
	x := int(lon / gridSize)
	y := int(lat / gridSize)
	return x*100000 + y
}

func (dt *DeviceTracker) buildSpatialIndex() {
	dt.spatialIndex = make(map[int][]int)

	for i := range dt.LocData {
		bounds := dt.LocData[i].Geometry.Bound()
		dt.LocData[i].Bounds = bounds

		minX := int(bounds.Min[0] / 0.01)
		maxX := int(bounds.Max[0] / 0.01)
		minY := int(bounds.Min[1] / 0.01)
		maxY := int(bounds.Max[1] / 0.01)

		for x := minX; x <= maxX; x++ {
			for y := minY; y <= maxY; y++ {
				key := x*100000 + y
				dt.spatialIndex[key] = append(dt.spatialIndex[key], i)
			}
		}
	}

	fmt.Printf("Built spatial index with %d cells\n", len(dt.spatialIndex))
}

func (dt *DeviceTracker) findIntersectingPolygons(lon, lat float64) []int {
	key := dt.getSpatialKey(lon, lat)
	return dt.spatialIndex[key]
}

func (dt *DeviceTracker) FindCampaignIntersectionForFolder(parquetFolder string) error {
	fmt.Println("(DT) Started step 3")
	startTime := time.Now()

	if err := dt.PrepareLocationDataFrame(); err != nil {
		return fmt.Errorf("failed to prepare location data: %w", err)
	}

	dt.buildSpatialIndex()

	var fileList []string
	var err error

	patterns := []string{
		filepath.Join(parquetFolder, "*.parquet"),
		filepath.Join(parquetFolder, "*.snappy.parquet"),
		filepath.Join(parquetFolder, "*.zstd.parquet"),
		filepath.Join(parquetFolder, "*.gzip.parquet"),
		filepath.Join(parquetFolder, "part-*.parquet"),
	}

	for _, pattern := range patterns {
		files, err := filepath.Glob(pattern)
		if err == nil && len(files) > 0 {
			fileList = files
			break
		}
	}

	if len(fileList) == 0 {
		fmt.Printf("No parquet files found with standard patterns, searching recursively in: %s\n", parquetFolder)
		err = filepath.Walk(parquetFolder, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".parquet") {
				fileList = append(fileList, path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if len(fileList) == 0 {
		fmt.Printf("DEBUG: Listing contents of %s\n", parquetFolder)
		entries, err := os.ReadDir(parquetFolder)
		if err != nil {
			return fmt.Errorf("cannot read directory %s: %w", parquetFolder, err)
		}

		fmt.Printf("Found %d items in directory:\n", len(entries))
		for i, entry := range entries {
			if i < 20 {
				fmt.Printf("  - %s (isDir: %v)\n", entry.Name(), entry.IsDir())
			}
		}
		if len(entries) > 20 {
			fmt.Printf("  ... and %d more items\n", len(entries)-20)
		}

		return fmt.Errorf("no parquet files found in %s", parquetFolder)
	}

	fmt.Printf("Total Files: %d\n", len(fileList))

	targetFolder := filepath.Join(dt.OutputFolder, filepath.Base(parquetFolder))
	os.MkdirAll(targetFolder, 0755)

	outCampaignCSV := filepath.Join(targetFolder, dt.OutCampaignDevices)
	os.Remove(outCampaignCSV)

	jobs := make(chan string, len(fileList))
	results := make(chan []DeviceRecord, workerPoolSize)
	var wg sync.WaitGroup

	for w := 0; w < workerPoolSize; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for parqFile := range jobs {
				records, err := dt.processCampaignFile(parqFile)
				if err != nil {
					fmt.Printf("Error processing %s: %v\n", filepath.Base(parqFile), err)
					continue
				}
				if len(records) > 0 {
					results <- records
				}
			}
		}()
	}

	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		dt.batchWriteCSV(outCampaignCSV, results)
	}()

	for i, file := range fileList {
		if i%100 == 0 {
			fmt.Printf("Queued: %d/%d files\n", i, len(fileList))
		}
		jobs <- file
	}
	close(jobs)

	wg.Wait()
	close(results)
	writerWg.Wait()

	if _, err := os.Stat(outCampaignCSV); err == nil {
		dt.removeCsvDuplicatesOptimized(outCampaignCSV)
	}

	fmt.Printf("(DT) Completed step 3 in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) batchWriteCSV(csvPath string, results <-chan []DeviceRecord) {
	file, err := os.Create(csvPath)
	if err != nil {
		fmt.Printf("Error creating CSV: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"device_id", "event_timestamp", "geometry", "address", "campaign"})

	batch := make([][]string, 0, csvBatchSize)
	totalRecords := 0

	for records := range results {
		for i := range records {
			row := []string{
				records[i].DeviceID,
				records[i].EventTimestamp.Format(time.RFC3339),
				fmt.Sprintf("POINT (%f %f)", records[i].Longitude, records[i].Latitude),
				records[i].Address,
				records[i].Campaign,
			}
			batch = append(batch, row)

			if len(batch) >= csvBatchSize {
				writer.WriteAll(batch)
				writer.Flush()
				totalRecords += len(batch)
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		writer.WriteAll(batch)
		writer.Flush()
		totalRecords += len(batch)
	}

	fmt.Printf("Total records written: %d\n", totalRecords)
}

func (dt *DeviceTracker) processCampaignFile(parqFilePath string) ([]DeviceRecord, error) {
	records, err := dt.readParquetOptimized(parqFilePath, []string{
		dt.DeviceIDColumn,
		dt.TimeColumnName,
		dt.LatColumn,
		dt.LonColumn,
	})
	if err != nil {
		return nil, err
	}

	// DEBUG: Print first record from each file
	/*if len(records) > 0 {
		fmt.Printf("DEBUG [%s]: First record - DeviceID=%s, Time=%s, Lat=%.6f, Lon=%.6f\n",
			filepath.Base(parqFilePath),
			records[0].DeviceID,
			records[0].EventTimestamp.Format(time.RFC3339),
			records[0].Latitude,
			records[0].Longitude)
	}*/

	dateStr := dt.extractDateFromPath(parqFilePath)
	insertDate, _ := time.Parse("20060102", dateStr)

	intersectRecords := make([]DeviceRecord, 0, len(records)/10)

	dt.LocDataMutex.RLock()
	defer dt.LocDataMutex.RUnlock()

	for i := range records {
		records[i].InsertDate = insertDate
		point := orb.Point{records[i].Longitude, records[i].Latitude}

		candidates := dt.findIntersectingPolygons(records[i].Longitude, records[i].Latitude)

		for _, idx := range candidates {
			if !dt.LocData[idx].Bounds.Contains(point) {
				continue
			}

			if planar.PolygonContains(dt.LocData[idx].Geometry, point) {
				records[i].Address = dt.LocData[idx].Address
				records[i].Campaign = dt.LocData[idx].Campaign
				intersectRecords = append(intersectRecords, records[i])
				break
			}
		}
	}

	return intersectRecords, nil
}

// Step 3.5: Process time-filtered data and insert to ClickHouse
func (dt *DeviceTracker) ProcessTimeFilteredData(parquetFolder string) error {
	fmt.Println("(DT) Started processing time-filtered data")
	startTime := time.Now()

	// Load device IDs from campaign intersection results
	if err := dt.loadDeviceIDFilter(); err != nil {
		return fmt.Errorf("failed to load device ID filter: %w", err)
	}

	var fileList []string
	patterns := []string{
		filepath.Join(parquetFolder, "*.parquet"),
		filepath.Join(parquetFolder, "*.snappy.parquet"),
		filepath.Join(parquetFolder, "*.zstd.parquet"),
		filepath.Join(parquetFolder, "*.gzip.parquet"),
	}

	for _, pattern := range patterns {
		files, err := filepath.Glob(pattern)
		if err == nil && len(files) > 0 {
			fileList = files
			break
		}
	}

	if len(fileList) == 0 {
		filepath.Walk(parquetFolder, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".parquet") {
				fileList = append(fileList, path)
			}
			return nil
		})
	}

	if len(fileList) == 0 {
		return fmt.Errorf("no parquet files found in %s", parquetFolder)
	}

	fmt.Printf("Processing %d files for time filtering\n", len(fileList))

	jobs := make(chan string, len(fileList))
	var wg sync.WaitGroup

	for w := 0; w < workerPoolSize; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for parqFile := range jobs {
				if err := dt.processTimeFilteredFile(parqFile); err != nil {
					fmt.Printf("Error processing %s: %v\n", filepath.Base(parqFile), err)
				}
			}
		}()
	}

	for i, file := range fileList {
		if i%100 == 0 {
			fmt.Printf("Queued for time filtering: %d/%d files\n", i, len(fileList))
		}
		jobs <- file
	}
	close(jobs)

	wg.Wait()

	// Flush any remaining records
	dt.chMutex.Lock()
	if len(dt.chBatch) > 0 {
		dt.flushClickHouseBatchUnsafe()
	}
	dt.chMutex.Unlock()

	fmt.Printf("(DT) Completed time filtering in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) processTimeFilteredFile(parqFilePath string) error {
	records, err := dt.readParquetOptimized(parqFilePath, []string{
		dt.DeviceIDColumn,
		dt.TimeColumnName,
		dt.LatColumn,
		dt.LonColumn,
	})
	if err != nil {
		return err
	}

	dateStr := dt.extractDateFromPath(parqFilePath)
	loadDate, _ := time.Parse("20060102", dateStr)

	for i := range records {
		// Check if device ID is in the filter
		if !dt.isDeviceIDInFilter(records[i].DeviceID) {
			continue
		}

		// Check if timestamp is within time filter
		if !dt.isWithinTimeFilter(records[i].EventTimestamp) {
			continue
		}

		// Add to ClickHouse batch
		tfRecord := TimeFilteredRecord{
			DeviceID:       records[i].DeviceID,
			EventTimestamp: records[i].EventTimestamp,
			Latitude:       records[i].Latitude,
			Longitude:      records[i].Longitude,
			LoadDate:       loadDate,
		}

		if err := dt.addToClickHouseBatch(tfRecord); err != nil {
			return fmt.Errorf("failed to add to ClickHouse batch: %w", err)
		}
	}

	return nil
}

func (dt *DeviceTracker) readParquetOptimized(filePath string, columns []string) ([]DeviceRecord, error) {
	pf, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	return dt.readParquetFromFile(pf, columns)
}

func (dt *DeviceTracker) readParquetFromFile(pf *file.Reader, columns []string) ([]DeviceRecord, error) {
	numRowGroups := pf.NumRowGroups()
	if numRowGroups == 0 {
		return nil, nil
	}

	totalRows := 0
	for i := 0; i < numRowGroups; i++ {
		totalRows += int(pf.RowGroup(i).NumRows())
	}

	records := make([]DeviceRecord, 0, totalRows)

	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		rgRecords, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}
		records = append(records, rgRecords...)
	}

	return records, nil
}

func (dt *DeviceTracker) readRowGroup(pf *file.Reader, rgIdx int) ([]DeviceRecord, error) {
	rg := pf.RowGroup(rgIdx)
	numRows := int(rg.NumRows())

	if numRows == 0 {
		return nil, nil
	}

	schema := pf.MetaData().Schema
	deviceIDIdx := -1
	timeIdx := -1
	latIdx := -1
	lonIdx := -1

	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		name := col.Name()

		switch name {
		case dt.DeviceIDColumn:
			deviceIDIdx = i
		case dt.TimeColumnName:
			timeIdx = i
		case dt.LatColumn:
			latIdx = i
		case dt.LonColumn:
			lonIdx = i
		}
	}

	if deviceIDIdx == -1 || timeIdx == -1 || latIdx == -1 || lonIdx == -1 {
		return nil, fmt.Errorf("required columns not found")
	}

	var wg sync.WaitGroup
	var deviceIDs []string
	var timestamps []time.Time
	var latitudes []float64
	var longitudes []float64

	wg.Add(4)

	go func() {
		defer wg.Done()
		deviceIDs = dt.readStringColumn(rg, deviceIDIdx, numRows)
	}()

	go func() {
		defer wg.Done()
		timestamps = dt.readTimestampColumn(rg, timeIdx, numRows)
	}()

	go func() {
		defer wg.Done()
		latitudes = dt.readFloatColumn(rg, latIdx, numRows)
	}()

	go func() {
		defer wg.Done()
		longitudes = dt.readFloatColumn(rg, lonIdx, numRows)
	}()

	wg.Wait()

	minLen := numRows
	if len(deviceIDs) < minLen {
		minLen = len(deviceIDs)
	}
	if len(timestamps) < minLen {
		minLen = len(timestamps)
	}
	if len(latitudes) < minLen {
		minLen = len(latitudes)
	}
	if len(longitudes) < minLen {
		minLen = len(longitudes)
	}

	records := make([]DeviceRecord, minLen)
	for i := 0; i < minLen; i++ {
		records[i] = DeviceRecord{
			DeviceID:       deviceIDs[i],
			EventTimestamp: timestamps[i],
			Latitude:       latitudes[i],
			Longitude:      longitudes[i],
		}
	}

	return records, nil
}

func (dt *DeviceTracker) readStringColumn(rg *file.RowGroupReader, colIdx int, numRows int) []string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic reading string column: %v\n", r)
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]string, 0)
	}

	result := make([]string, 0, numRows)

	switch reader := col.(type) {
	case *file.ByteArrayColumnChunkReader:
		values := make([]parquet.ByteArray, 8192)
		defLevels := make([]int16, 8192)

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if defLevels[i] > 0 {
					result = append(result, string(values[i]))
				} else {
					result = append(result, "")
				}
			}
		}
	}

	return result
}

func (dt *DeviceTracker) readTimestampColumn(rg *file.RowGroupReader, colIdx int, numRows int) []time.Time {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic reading timestamp column: %v\n", r)
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]time.Time, 0)
	}

	result := make([]time.Time, 0, numRows)

	switch reader := col.(type) {
	case *file.Int64ColumnChunkReader:
		values := make([]int64, 8192)
		defLevels := make([]int16, 8192)

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if defLevels[i] > 0 {
					result = append(result, time.Unix(0, values[i]*1000))
				} else {
					result = append(result, time.Time{})
				}
			}
		}
	}

	return result
}

func (dt *DeviceTracker) readFloatColumn(rg *file.RowGroupReader, colIdx int, numRows int) []float64 {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic reading float column: %v\n", r)
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]float64, 0)
	}

	result := make([]float64, 0, numRows)

	switch reader := col.(type) {
	case *file.Float64ColumnChunkReader:
		values := make([]float64, 8192)
		defLevels := make([]int16, 8192)

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if defLevels[i] > 0 {
					result = append(result, values[i])
				} else {
					result = append(result, 0.0)
				}
			}
		}
	case *file.Float32ColumnChunkReader:
		values := make([]float32, 8192)
		defLevels := make([]int16, 8192)

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if defLevels[i] > 0 {
					result = append(result, float64(values[i]))
				} else {
					result = append(result, 0.0)
				}
			}
		}
	}

	return result
}

// Step 4: Merge campaign intersections - PARALLEL
func (dt *DeviceTracker) MergeCampaignIntersectionsCSV(folderList []string, folderPrefix string) error {
	fmt.Println("(DT) Started step 4")
	startTime := time.Now()

	outCSV := filepath.Join(dt.OutputFolder, dt.OutCampaignDevices)
	allCSV := filepath.Join(dt.OutputFolder, "All_Devices_Within_Campaign.csv")
	os.Remove(outCSV)
	os.Remove(allCSV)

	type result struct {
		records []DeviceRecord
		err     error
	}

	results := make(chan result, len(folderList))
	var wg sync.WaitGroup

	for _, folder := range folderList {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()

			targetFolder := filepath.Join(dt.OutputFolder, folderPrefix+f)
			inCSV := filepath.Join(targetFolder, dt.OutCampaignDevices)

			records, err := dt.readDeviceCSV(inCSV)
			if err != nil {
				results <- result{nil, err}
				return
			}

			dateStr := f[len(f)-8:]
			insertDate, _ := time.Parse("20060102", dateStr)

			for i := range records {
				records[i].InsertDate = insertDate
			}

			results <- result{records, nil}
		}(folder)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allRecords []DeviceRecord
	for res := range results {
		if res.err == nil && res.records != nil {
			allRecords = append(allRecords, res.records...)
		}
	}

	dt.writeDeviceCSVOptimized(allCSV, allRecords)

	seen := make(map[string]struct{}, len(allRecords))
	uniqueRecords := make([]DeviceRecord, 0, len(allRecords)/2)

	for i := range allRecords {
		if _, exists := seen[allRecords[i].DeviceID]; !exists {
			seen[allRecords[i].DeviceID] = struct{}{}
			uniqueRecords = append(uniqueRecords, allRecords[i])
		}
	}

	dt.writeDeviceCSVOptimized(outCSV, uniqueRecords)

	fmt.Printf("Total: %d, Duration: %v\n", len(uniqueRecords), time.Since(startTime))
	fmt.Println("(DT) Completed step 4")
	return nil
}

// Step 6: Idle device search - PARALLEL (reads from ClickHouse)
func (dt *DeviceTracker) RunIdleDeviceSearch(targetDates []string) error {
	fmt.Println("(DT) Started step 6")
	startTime := time.Now()

	for _, date := range targetDates {
		fmt.Printf("Processing date: %s\n", date)
		err := dt.findIdleDevicesFromClickHouse(date)
		if err != nil {
			fmt.Printf("Error for %s: %v\n", date, err)
		}
	}

	err := dt.mergeIdleDevicesOutput(targetDates)
	if err != nil {
		return err
	}

	fmt.Printf("(DT) Completed step 6 in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) findIdleDevicesFromClickHouse(targetDate string) error {
	// Load unique device data from campaign CSV
	uIdDataFrame, err := dt.getUniqIdDataFrame()
	if err != nil {
		return err
	}

	uniqDeviceMap := make(map[string]DeviceRecord)
	for i := range uIdDataFrame {
		uniqDeviceMap[uIdDataFrame[i].DeviceID] = uIdDataFrame[i]
	}

	// Query ClickHouse for time-filtered data for this date
	parseDate, _ := time.Parse("2006-01-02", targetDate)

	query := `
		SELECT device_id, event_timestamp, latitude, longitude
		FROM time_filtered
		WHERE toDate(load_date) = ?
		ORDER BY device_id, event_timestamp
	`

	rows, err := dt.clickhouseConn.Query(query, parseDate)
	if err != nil {
		return fmt.Errorf("failed to query ClickHouse: %w", err)
	}
	defer rows.Close()

	// Read all records and group by device_id
	deviceGroups := make(map[string][]DeviceRecord)

	for rows.Next() {
		var rec DeviceRecord
		if err := rows.Scan(&rec.DeviceID, &rec.EventTimestamp, &rec.Latitude, &rec.Longitude); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		deviceGroups[rec.DeviceID] = append(deviceGroups[rec.DeviceID], rec)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("Loaded %d devices from ClickHouse for date %s\n", len(deviceGroups), targetDate)

	if len(deviceGroups) == 0 {
		return nil
	}

	// Process devices in parallel to find idle ones
	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}

	idleDevicesChan := make(chan DeviceRecord, len(deviceIDs))
	var processWg sync.WaitGroup

	workerCount := runtime.NumCPU()
	chunkSize := (len(deviceIDs) + workerCount - 1) / workerCount

	for w := 0; w < workerCount; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(deviceIDs) {
			end = len(deviceIDs)
		}
		if start >= len(deviceIDs) {
			break
		}

		processWg.Add(1)
		go func(deviceIDChunk []string) {
			defer processWg.Done()

			for _, deviceID := range deviceIDChunk {
				deviceDF := deviceGroups[deviceID]
				if len(deviceDF) == 0 {
					continue
				}

				campRec, exists := uniqDeviceMap[deviceID]
				if !exists {
					continue
				}

				firstRec := deviceDF[0]
				firstPoint := orb.Point{firstRec.Longitude, firstRec.Latitude}
				bufferPolygon := dt.createBufferOptimized(firstPoint, dt.IdleDeviceBuffer)

				isIdle := true
				for i := range deviceDF {
					point := orb.Point{deviceDF[i].Longitude, deviceDF[i].Latitude}
					if !planar.PolygonContains(bufferPolygon, point) {
						isIdle = false
						break
					}
				}

				if isIdle {
					idleRec := firstRec
					idleRec.Campaign = campRec.Campaign
					idleRec.Address = campRec.Address
					idleRec.EventTimestamp = campRec.EventTimestamp

					idleDevicesChan <- idleRec
				}
			}
		}(deviceIDs[start:end])
	}

	go func() {
		processWg.Wait()
		close(idleDevicesChan)
	}()

	idleDevices := make([]DeviceRecord, 0)
	for rec := range idleDevicesChan {
		idleDevices = append(idleDevices, rec)
	}

	outCSV := filepath.Join(dt.OutputFolder, fmt.Sprintf("%s_%s.csv", dt.OutTargetDevices, targetDate))
	if len(idleDevices) > 0 {
		dt.writeIdleDeviceCSV(outCSV, idleDevices)
		dt.removeCsvDuplicatesOptimized(outCSV)
	}

	fmt.Printf("Idle devices found: %d\n", len(idleDevices))
	return nil
}

func (dt *DeviceTracker) mergeIdleDevicesOutput(targetDates []string) error {
	outCSV := filepath.Join(dt.OutputFolder, dt.OutTargetDevices+".csv")

	var allRecords []DeviceRecord

	for _, targetDate := range targetDates {
		inCSV := filepath.Join(dt.OutputFolder, fmt.Sprintf("%s_%s.csv", dt.OutTargetDevices, targetDate))
		if _, err := os.Stat(inCSV); err == nil {
			records, err := dt.readDeviceCSV(inCSV)
			if err == nil {
				allRecords = append(allRecords, records...)
			}
		}
	}

	if len(allRecords) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	unique := make([]DeviceRecord, 0, len(allRecords))

	for i := range allRecords {
		key := allRecords[i].DeviceID + "|" + allRecords[i].Campaign
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, allRecords[i])
		}
	}

	return dt.writeIdleDeviceCSV(outCSV, unique)
}

func (dt *DeviceTracker) PrepareLocationDataFrame() error {
	files, err := filepath.Glob(filepath.Join(dt.LocFolder, "*.csv"))
	if err != nil {
		return err
	}

	dt.LocDataMutex.Lock()
	defer dt.LocDataMutex.Unlock()

	dt.LocData = make([]LocationRecord, 0, 1000)

	for _, csvPath := range files {
		file, err := os.Open(csvPath)
		if err != nil {
			continue
		}

		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		file.Close()

		if err != nil {
			continue
		}

		campaign := strings.TrimPrefix(filepath.Base(csvPath), "Geocoded_")
		campaign = strings.TrimSuffix(campaign, ".csv")

		for i := 1; i < len(records); i++ {
			if len(records[i]) < 2 {
				continue
			}

			loc := LocationRecord{
				Address:  records[i][0],
				Campaign: campaign,
				Geometry: dt.parseWKTPolygon(records[i][1]),
			}
			dt.LocData = append(dt.LocData, loc)
		}
	}

	fmt.Printf("Loaded %d location records\n", len(dt.LocData))
	return nil
}

func (dt *DeviceTracker) parseWKTPolygon(wkt string) orb.Polygon {
	wkt = strings.TrimPrefix(wkt, "POLYGON ((")
	wkt = strings.TrimSuffix(wkt, "))")
	wkt = strings.TrimSpace(wkt)

	points := strings.Split(wkt, ",")
	ring := make(orb.Ring, 0, len(points))

	for _, point := range points {
		coords := strings.Fields(strings.TrimSpace(point))
		if len(coords) >= 2 {
			lon, _ := strconv.ParseFloat(coords[0], 64)
			lat, _ := strconv.ParseFloat(coords[1], 64)
			ring = append(ring, orb.Point{lon, lat})
		}
	}

	return orb.Polygon{ring}
}

func (dt *DeviceTracker) createBufferOptimized(point orb.Point, meters float64) orb.Polygon {
	degreeOffset := meters / 111320.0

	ring := orb.Ring{
		{point[0] - degreeOffset, point[1] - degreeOffset},
		{point[0] + degreeOffset, point[1] - degreeOffset},
		{point[0] + degreeOffset, point[1] + degreeOffset},
		{point[0] - degreeOffset, point[1] + degreeOffset},
		{point[0] - degreeOffset, point[1] - degreeOffset},
	}

	return orb.Polygon{ring}
}

func (dt *DeviceTracker) extractDateFromPath(path string) string {
	parts := strings.Split(path, string(filepath.Separator))
	for _, part := range parts {
		if strings.HasPrefix(part, "load_date=") {
			return strings.TrimPrefix(part, "load_date=")
		}
	}
	return ""
}

func (dt *DeviceTracker) readDeviceCSV(filePath string) ([]DeviceRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	records := make([]DeviceRecord, 0, len(rows)-1)

	for i := 1; i < len(rows); i++ {
		if len(rows[i]) < 5 {
			continue
		}

		timestamp, _ := time.Parse(time.RFC3339, rows[i][1])

		geometry := rows[i][2]
		lat, lon := dt.parsePointGeometry(geometry)

		rec := DeviceRecord{
			DeviceID:       rows[i][0],
			EventTimestamp: timestamp,
			Latitude:       lat,
			Longitude:      lon,
			Address:        rows[i][3],
			Campaign:       rows[i][4],
		}
		records = append(records, rec)
	}

	return records, nil
}

func (dt *DeviceTracker) parsePointGeometry(geometry string) (float64, float64) {
	geometry = strings.TrimPrefix(geometry, "POINT (")
	geometry = strings.TrimSuffix(geometry, ")")
	parts := strings.Fields(geometry)

	if len(parts) >= 2 {
		lon, _ := strconv.ParseFloat(parts[0], 64)
		lat, _ := strconv.ParseFloat(parts[1], 64)
		return lat, lon
	}

	return 0.0, 0.0
}

func (dt *DeviceTracker) writeDeviceCSVOptimized(filePath string, records []DeviceRecord) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"device_id", "event_timestamp", "geometry", "address", "campaign"})

	batch := make([][]string, 0, csvBatchSize)

	for i := range records {
		row := []string{
			records[i].DeviceID,
			records[i].EventTimestamp.Format(time.RFC3339),
			fmt.Sprintf("POINT (%f %f)", records[i].Longitude, records[i].Latitude),
			records[i].Address,
			records[i].Campaign,
		}
		batch = append(batch, row)

		if len(batch) >= csvBatchSize {
			writer.WriteAll(batch)
			writer.Flush()
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		writer.WriteAll(batch)
	}

	return nil
}

func (dt *DeviceTracker) writeIdleDeviceCSV(filePath string, records []DeviceRecord) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"device_id", "visited_time", "address", "campaign", "geometry"})

	batch := make([][]string, 0, csvBatchSize)

	for i := range records {
		row := []string{
			records[i].DeviceID,
			records[i].EventTimestamp.Format(time.RFC3339),
			records[i].Address,
			records[i].Campaign,
			fmt.Sprintf("POINT (%f %f)", records[i].Longitude, records[i].Latitude),
		}
		batch = append(batch, row)

		if len(batch) >= csvBatchSize {
			writer.WriteAll(batch)
			writer.Flush()
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		writer.WriteAll(batch)
	}

	return nil
}

func (dt *DeviceTracker) removeCsvDuplicatesOptimized(csvPath string) error {
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		return nil
	}

	records, err := dt.readDeviceCSV(csvPath)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(records))
	unique := make([]DeviceRecord, 0, len(records))

	for i := range records {
		if _, exists := seen[records[i].DeviceID]; !exists {
			seen[records[i].DeviceID] = struct{}{}
			unique = append(unique, records[i])
		}
	}

	return dt.writeDeviceCSVOptimized(csvPath, unique)
}

func (dt *DeviceTracker) deduplicateRecords(records []DeviceRecord) []DeviceRecord {
	seen := make(map[string]struct{}, len(records))
	unique := make([]DeviceRecord, 0, len(records))

	for i := range records {
		key := records[i].DeviceID + records[i].EventTimestamp.Format(time.RFC3339)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, records[i])
		}
	}

	return unique
}

func (dt *DeviceTracker) getUniqueIDList() ([]string, error) {
	outCSV := filepath.Join(dt.OutputFolder, dt.OutCampaignDevices)
	records, err := dt.readDeviceCSV(outCSV)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(records))
	ids := make([]string, 0, len(records))

	for i := range records {
		if _, exists := seen[records[i].DeviceID]; !exists {
			seen[records[i].DeviceID] = struct{}{}
			ids = append(ids, records[i].DeviceID)
		}
	}

	return ids, nil
}

func (dt *DeviceTracker) getUniqIdDataFrame() ([]DeviceRecord, error) {
	outCSV := filepath.Join(dt.OutputFolder, dt.OutCampaignDevices)
	return dt.readDeviceCSV(outCSV)
}

func RunDeviceTracker(skipTimezoneError bool, runForPastDays bool, runSteps []int) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	today := time.Now()

	dates := []string{
		"2025-10-03",
	}

	folderList := make([]string, 0, len(dates))
	for _, d := range dates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		folderList = append(folderList, folder)
	}

	fmt.Printf("Folder List: %v\n", folderList)
	fmt.Printf("CPU Cores: %d\n", runtime.NumCPU())

	chConfig := ClickHouseConfig{
		Host:     "172.173.97.164",
		Port:     9000,
		Database: "device_tracking",
		Username: "default",
		Password: "nyros",
	}

	dt, err := NewDeviceTracker(
		"/home/device-tracker/data/geocoded",
		"/home/device-tracker/data/output",
		chConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to create device tracker: %w", err)
	}
	defer dt.Close()

	// Set target dates for time filtering
	if err := dt.setTargetDates(dates); err != nil {
		return fmt.Errorf("failed to set target dates: %w", err)
	}

	if containsStep(runSteps, 3) {
		fmt.Println("\n========== Running STEP 3 ==========")
		if runForPastDays {
			for _, folder := range folderList {
				err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/" + folder)
				if err != nil {
					fmt.Printf("Error in step 3 for %s: %v\n", folder, err)
				}
			}
		} else {
			todayFolder := "load_date=" + today.Format("20060102")
			err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/" + todayFolder)
			if err != nil {
				fmt.Printf("Error in step 3: %v\n", err)
			}
		}
		fmt.Println("Step 3 Completed")
	}

	if containsStep(runSteps, 4) {
		fmt.Println("\n========== Running STEP 4 ==========")
		err := dt.MergeCampaignIntersectionsCSV(folderList, "")
		if err != nil {
			fmt.Printf("Error in step 4: %v\n", err)
		}
		fmt.Println("Step 4 Completed")
	}

	// New Step 3.5: Process time-filtered data and insert to ClickHouse
	if containsStep(runSteps, 5) {
		fmt.Println("\n========== Running STEP 5 (Time Filtering to ClickHouse) ==========")
		if runForPastDays {
			for _, folder := range folderList {
				err := dt.ProcessTimeFilteredData("/mnt/blobcontainer/" + folder)
				if err != nil {
					fmt.Printf("Error in step 5 for %s: %v\n", folder, err)
				}
			}
		} else {
			todayFolder := "load_date=" + today.Format("20060102")
			err := dt.ProcessTimeFilteredData("/mnt/blobcontainer/" + todayFolder)
			if err != nil {
				fmt.Printf("Error in step 5: %v\n", err)
			}
		}
		fmt.Println("Step 5 Completed")
	}

	if containsStep(runSteps, 6) {
		fmt.Println("\n========== Running STEP 6 ==========")
		err := dt.RunIdleDeviceSearch(dates)
		if err != nil {
			fmt.Printf("Error in step 6: %v\n", err)
			return err
		}
		fmt.Println("Step 6 Completed")
	}

	return nil
}

func containsStep(steps []int, step int) bool {
	for _, s := range steps {
		if s == step {
			return true
		}
	}
	return false
}

func main() {
	fmt.Println("===========================================")
	fmt.Println("   Device Tracker - High Performance")
	fmt.Println("===========================================")

	startTime := time.Now()

	runSteps := []int{3, 4, 5, 6}

	err := RunDeviceTracker(true, true, runSteps)
	if err != nil {
		fmt.Printf("\n❌ Error: %v\n", err)
		os.Exit(1)
	}

	duration := time.Since(startTime)
	fmt.Printf("\n===========================================\n")
	fmt.Printf("✅ Process Completed Successfully\n")
	fmt.Printf("⏱️  Total Duration: %v\n", duration)
	fmt.Printf("===========================================\n")
}
