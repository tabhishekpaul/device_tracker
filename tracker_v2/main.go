package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

const (
	csvBatchSize = 10000
)

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

	OutputFolder string

	OutCampaignDevices string
	OutTargetDevices   string
	OutTimeFiltered    string

	IdleDeviceBuffer float64
	NumWorkers       int
}

type LocationRecord struct {
	Address  string
	Campaign string
	Geometry orb.Polygon
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

func NewDeviceTracker(locFolder, outputFolder string) *DeviceTracker {
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

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
		OutTimeFiltered:    "Time_Filtered",
		IdleDeviceBuffer:   10.0,
		NumWorkers:         numWorkers,
	}
}

// Step 3: Campaign intersection detection - SEQUENTIAL like Python
func (dt *DeviceTracker) FindCampaignIntersectionForFolder(parquetFolder string) error {
	fmt.Println("(DT) Started step 3")
	startTime := time.Now()

	if err := dt.PrepareLocationDataFrame(); err != nil {
		return fmt.Errorf("failed to prepare location data: %w", err)
	}

	fileList, err := filepath.Glob(filepath.Join(parquetFolder, "*.parquet"))
	if err != nil {
		return err
	}

	fmt.Printf("Total Files: %d\n", len(fileList))

	targetFolder := filepath.Join(dt.OutputFolder, filepath.Base(parquetFolder))
	os.MkdirAll(targetFolder, 0755)

	outCampaignCSV := filepath.Join(targetFolder, dt.OutCampaignDevices)
	os.Remove(outCampaignCSV)

	// Process files sequentially like Python's multiprocessing approach
	for i, parqFile := range fileList {
		fmt.Printf("%d -- %s\n", i+1, filepath.Base(parqFile))

		records, err := dt.processCampaignFile(parqFile)
		if err != nil {
			fmt.Printf("Error processing %s: %v\n", filepath.Base(parqFile), err)
			continue
		}

		if len(records) > 0 {
			dt.appendToCSV(outCampaignCSV, records)
		}
	}

	if _, err := os.Stat(outCampaignCSV); err == nil {
		dt.removeCsvDuplicatesOptimized(outCampaignCSV)
	}

	fmt.Printf("(DT) Completed step 3 in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) appendToCSV(csvPath string, records []DeviceRecord) error {
	fileExists := false
	if _, err := os.Stat(csvPath); err == nil {
		fileExists = true
	}

	file, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExists {
		writer.Write([]string{"device_id", "event_timestamp", "geometry", "address", "campaign"})
	}

	for i := range records {
		row := []string{
			records[i].DeviceID,
			records[i].EventTimestamp.Format(time.RFC3339),
			fmt.Sprintf("POINT (%f %f)", records[i].Longitude, records[i].Latitude),
			records[i].Address,
			records[i].Campaign,
		}
		writer.Write(row)
	}

	return nil
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

	// Extract date from path - handles both "load_date=20231003" format
	dateStr := dt.extractDateFromPath(parqFilePath)
	insertDate, _ := time.Parse("20060102", dateStr)

	fmt.Printf("Total Records -- %d\n", len(records))

	intersectRecords := make([]DeviceRecord, 0, len(records)/10)

	dt.LocDataMutex.RLock()
	defer dt.LocDataMutex.RUnlock()

	for i := range records {
		records[i].InsertDate = insertDate
		point := orb.Point{records[i].Longitude, records[i].Latitude}

		for j := range dt.LocData {
			if planar.PolygonContains(dt.LocData[j].Geometry, point) {
				records[i].Address = dt.LocData[j].Address
				records[i].Campaign = dt.LocData[j].Campaign
				intersectRecords = append(intersectRecords, records[i])
				break
			}
		}
	}

	return intersectRecords, nil
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

	records := make([]DeviceRecord, 0, 1000)

	// Read row groups one at a time
	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		rgRecords, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			// Skip problematic row groups
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

	records := make([]DeviceRecord, 0, numRows)

	// Find column indices
	schema := pf.MetaData().Schema
	deviceIDIdx := -1
	timeIdx := -1
	latIdx := -1
	lonIdx := -1

	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		name := col.Name()

		if name == dt.DeviceIDColumn {
			deviceIDIdx = i
		} else if name == dt.TimeColumnName {
			timeIdx = i
		} else if name == dt.LatColumn {
			latIdx = i
		} else if name == dt.LonColumn {
			lonIdx = i
		}
	}

	if deviceIDIdx == -1 || timeIdx == -1 || latIdx == -1 || lonIdx == -1 {
		return nil, fmt.Errorf("required columns not found")
	}

	// Read columns
	deviceIDs := dt.readStringColumn(rg, deviceIDIdx, numRows)
	timestamps := dt.readTimestampColumn(rg, timeIdx, numRows)
	latitudes := dt.readFloatColumn(rg, latIdx, numRows)
	longitudes := dt.readFloatColumn(rg, lonIdx, numRows)

	// Build records
	for i := 0; i < numRows; i++ {
		if i < len(deviceIDs) && i < len(timestamps) && i < len(latitudes) && i < len(longitudes) {
			rec := DeviceRecord{
				DeviceID:       deviceIDs[i],
				EventTimestamp: timestamps[i],
				Latitude:       latitudes[i],
				Longitude:      longitudes[i],
			}
			records = append(records, rec)
		}
	}

	return records, nil
}

func (dt *DeviceTracker) readStringColumn(rg *file.RowGroupReader, colIdx int, numRows int) []string {
	defer func() {
		if r := recover(); r != nil {
			// Recover from panics
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]string, numRows)
	}

	result := make([]string, 0, numRows)

	switch reader := col.(type) {
	case *file.ByteArrayColumnChunkReader:
		values := make([]parquet.ByteArray, 1024)
		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, nil, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				result = append(result, string(values[i]))
			}
		}
	}

	return result
}

func (dt *DeviceTracker) readTimestampColumn(rg *file.RowGroupReader, colIdx int, numRows int) []time.Time {
	defer func() {
		if r := recover(); r != nil {
			// Recover from panics
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]time.Time, numRows)
	}

	result := make([]time.Time, 0, numRows)

	switch reader := col.(type) {
	case *file.Int64ColumnChunkReader:
		values := make([]int64, 1024)
		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, nil, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				// Assume microseconds since epoch
				result = append(result, time.Unix(0, values[i]*1000))
			}
		}
	}

	return result
}

func (dt *DeviceTracker) readFloatColumn(rg *file.RowGroupReader, colIdx int, numRows int) []float64 {
	defer func() {
		if r := recover(); r != nil {
			// Recover from panics
		}
	}()

	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]float64, numRows)
	}

	result := make([]float64, 0, numRows)

	switch reader := col.(type) {
	case *file.Float64ColumnChunkReader:
		values := make([]float64, 1024)
		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, nil, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				result = append(result, values[i])
			}
		}
	case *file.Float32ColumnChunkReader:
		values := make([]float32, 1024)
		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, nil, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				result = append(result, float64(values[i]))
			}
		}
	}

	return result
}

// Step 4: Merge campaign intersections
func (dt *DeviceTracker) MergeCampaignIntersectionsCSV(folderList []string, folderPrefix string) error {
	fmt.Println("(DT) Started step 4")
	startTime := time.Now()

	outCSV := filepath.Join(dt.OutputFolder, dt.OutCampaignDevices)
	allCSV := filepath.Join(dt.OutputFolder, "All_Devices_Within_Campaign.csv")
	os.Remove(outCSV)
	os.Remove(allCSV)

	var allRecords []DeviceRecord

	for _, folder := range folderList {
		targetFolder := filepath.Join(dt.OutputFolder, folderPrefix+folder)
		inCSV := filepath.Join(targetFolder, dt.OutCampaignDevices)

		records, err := dt.readDeviceCSV(inCSV)
		if err != nil {
			continue
		}

		// Extract date from folder name
		dateStr := folder[len(folder)-8:]
		insertDate, _ := time.Parse("20060102", dateStr)

		for i := range records {
			records[i].InsertDate = insertDate
		}

		allRecords = append(allRecords, records...)
	}

	// Write all devices before deduplication
	dt.writeDeviceCSVOptimized(allCSV, allRecords)

	// Deduplicate by device_id only
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

// Step 5: Filter by time - with concurrent processing
func (dt *DeviceTracker) FilterTargetTime(dateFolder string, targetDates []string, skipTimezoneError bool) error {
	fmt.Println("(DT) Started step 5")
	startTime := time.Now()

	targetFolder := filepath.Join(dt.OutputFolder, filepath.Base(dateFolder))
	os.MkdirAll(targetFolder, 0755)

	fileList, err := filepath.Glob(filepath.Join(dateFolder, "/*.parquet"))
	if err != nil {
		return err
	}

	idList, err := dt.getUniqueIDList()
	if err != nil {
		return err
	}

	fmt.Printf("Devices to match: %d, Files: %d\n", len(idList), len(fileList))

	idSet := make(map[string]struct{}, len(idList))
	for _, id := range idList {
		idSet[id] = struct{}{}
	}

	for _, targetDate := range targetDates {
		outCSV := filepath.Join(targetFolder, fmt.Sprintf("%s_%s.csv", dt.OutTimeFiltered, targetDate))
		os.Remove(outCSV)

		startTimeFilter, _ := time.Parse("2006-01-02 15:04:05", targetDate+" "+dt.FilterInTime)
		endTimeFilter, _ := time.Parse("2006-01-02 15:04:05", targetDate+" "+dt.FilterOutTime)

		var allFiltered []DeviceRecord

		for _, parqFile := range fileList {
			records, err := dt.readParquetOptimized(parqFile, nil)
			if err != nil {
				if !skipTimezoneError {
					fmt.Printf("Error: %v\n", err)
				}
				continue
			}

			filtered := make([]DeviceRecord, 0)
			for i := range records {
				if _, exists := idSet[records[i].DeviceID]; exists {
					if records[i].EventTimestamp.After(startTimeFilter) && records[i].EventTimestamp.Before(endTimeFilter) {
						filtered = append(filtered, records[i])
					}
				}
			}

			allFiltered = append(allFiltered, filtered...)
		}

		allFiltered = dt.deduplicateRecords(allFiltered)
		fmt.Printf("Date %s: %d records\n", targetDate, len(allFiltered))

		dt.writeDeviceCSVOptimized(outCSV, allFiltered)
	}

	fmt.Printf("(DT) Completed step 5 in %v\n", time.Since(startTime))
	return nil
}

// Step 6: Idle device search
func (dt *DeviceTracker) RunIdleDeviceSearch(folderList, targetDates []string) error {
	fmt.Println("(DT) Started step 6")
	startTime := time.Now()

	for _, date := range targetDates {
		fmt.Printf("Processing date: %s\n", date)
		err := dt.findIdleDevicesOptimized(folderList, date)
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

func (dt *DeviceTracker) findIdleDevicesOptimized(folderList []string, targetDate string) error {
	uIdDataFrame, err := dt.getUniqIdDataFrame()
	if err != nil {
		return err
	}

	// Create map for quick lookup with deduplication
	uniqDeviceMap := make(map[string]DeviceRecord)
	for i := range uIdDataFrame {
		uniqDeviceMap[uIdDataFrame[i].DeviceID] = uIdDataFrame[i]
	}

	var allRecords []DeviceRecord

	for _, folderName := range folderList {
		csvPath := filepath.Join(dt.OutputFolder, folderName, fmt.Sprintf("%s_%s.csv", dt.OutTimeFiltered, targetDate))
		if _, err := os.Stat(csvPath); os.IsNotExist(err) {
			continue
		}

		records, err := dt.readDeviceCSV(csvPath)
		if err != nil {
			continue
		}

		allRecords = append(allRecords, records...)
	}

	if len(allRecords) == 0 {
		return nil
	}

	// Group by device ID
	deviceGroups := make(map[string][]DeviceRecord)
	for i := range allRecords {
		id := allRecords[i].DeviceID
		deviceGroups[id] = append(deviceGroups[id], allRecords[i])
	}

	idleDevices := make([]DeviceRecord, 0)

	for deviceID, deviceDF := range deviceGroups {
		if len(deviceDF) == 0 {
			continue
		}

		campRec, exists := uniqDeviceMap[deviceID]
		if !exists {
			//fmt.Printf("[WARN] No metadata found for device_id=%s on date=%s\n", deviceID, targetDate)
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
			idleRec.EventTimestamp = campRec.EventTimestamp // visited_time

			idleDevices = append(idleDevices, idleRec)
		}
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

	// Deduplicate by device_id AND campaign
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

		rec := DeviceRecord{
			DeviceID:       rows[i][0],
			EventTimestamp: timestamp,
			Address:        rows[i][3],
			Campaign:       rows[i][4],
		}
		records = append(records, rec)
	}

	return records, nil
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
		"2023-10-03",
	}

	folderList := make([]string, 0, len(dates))
	for _, d := range dates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		folderList = append(folderList, folder)
	}

	fmt.Printf("Folder List: %v\n", folderList)
	fmt.Printf("CPU Cores: %d\n", runtime.NumCPU())

	dt := NewDeviceTracker(
		"/home/device-tracker/data/geocoded",
		"/home/device-tracker/data/output",
	)

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

	if containsStep(runSteps, 5) {
		fmt.Println("\n========== Running STEP 5 ==========")
		dt.FilterInTime = "02:00:00"
		dt.FilterOutTime = "04:30:00"

		for _, folder := range folderList {
			err := dt.FilterTargetTime("/mnt/blobcontainer/"+folder, dates, skipTimezoneError)
			if err != nil {
				fmt.Printf("Error in step 5 for %s: %v\n", folder, err)
			}
		}
		fmt.Println("Step 5 Completed")
	}

	if containsStep(runSteps, 6) {
		fmt.Println("\n========== Running STEP 6 ==========")
		err := dt.RunIdleDeviceSearch(folderList, dates)
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

	err := RunDeviceTracker(true, false, runSteps)
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
