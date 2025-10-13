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

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

const (
	jobBufferSize    = 1000
	resultBufferSize = 1000
	csvBatchSize     = 10000
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

	allocator memory.Allocator
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
		allocator:          memory.NewGoAllocator(),
		ctx:                context.Background(),
	}
}

// Step 3: Campaign intersection detection
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

	fmt.Printf("Total Files: %d, Workers: %d\n", len(fileList), dt.NumWorkers)

	targetFolder := filepath.Join(dt.OutputFolder, filepath.Base(parquetFolder))
	os.MkdirAll(targetFolder, 0755)

	outCampaignCSV := filepath.Join(targetFolder, dt.OutCampaignDevices)
	os.Remove(outCampaignCSV)

	jobs := make(chan string, jobBufferSize)
	results := make(chan []DeviceRecord, resultBufferSize)

	var wg sync.WaitGroup
	var writerWg sync.WaitGroup

	for w := 0; w < dt.NumWorkers; w++ {
		wg.Add(1)
		go dt.campaignWorker(&wg, jobs, results)
	}

	writerWg.Add(1)
	go dt.csvWriter(&writerWg, outCampaignCSV, results)

	go func() {
		for i, file := range fileList {
			if i%100 == 0 {
				fmt.Printf("Queued: %d/%d files\n", i, len(fileList))
			}
			jobs <- file
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	writerWg.Wait()

	dt.removeCsvDuplicatesOptimized(outCampaignCSV)

	fmt.Printf("(DT) Completed step 3 in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) campaignWorker(wg *sync.WaitGroup, jobs <-chan string, results chan<- []DeviceRecord) {
	defer wg.Done()

	for parqFile := range jobs {
		records, err := dt.processCampaignFile(parqFile)
		if err != nil {
			fmt.Printf("Error processing %s: %v\n", filepath.Base(parqFile), err)
		} else if len(records) > 0 {
			results <- records
		}
	}
}

func (dt *DeviceTracker) csvWriter(wg *sync.WaitGroup, csvPath string, results <-chan []DeviceRecord) {
	defer wg.Done()

	file, err := os.Create(csvPath)
	if err != nil {
		fmt.Printf("Error creating CSV: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
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

	dateStr := dt.extractDateFromPath(parqFilePath)
	insertDate, _ := time.Parse("20060102", dateStr)

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
	/*f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()*/

	pf, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, dt.allocator)
	if err != nil {
		return nil, err
	}

	table, err := reader.ReadTable(dt.ctx)
	if err != nil {
		return nil, err
	}
	defer table.Release()

	records := dt.extractRecordsOptimized(table)
	return records, nil
}

func (dt *DeviceTracker) extractRecordsOptimized(table arrow.Table) []DeviceRecord {
	schema := table.Schema()
	numRows := int(table.NumRows())

	deviceIDIdx := schema.FieldIndices(dt.DeviceIDColumn)
	timeIdx := schema.FieldIndices(dt.TimeColumnName)
	latIdx := schema.FieldIndices(dt.LatColumn)
	lonIdx := schema.FieldIndices(dt.LonColumn)

	if len(deviceIDIdx) == 0 || len(timeIdx) == 0 || len(latIdx) == 0 || len(lonIdx) == 0 {
		return nil
	}

	records := make([]DeviceRecord, 0, numRows)

	deviceIDCol := table.Column(deviceIDIdx[0]).Data()
	timeCol := table.Column(timeIdx[0]).Data()
	latCol := table.Column(latIdx[0]).Data()
	lonCol := table.Column(lonIdx[0]).Data()

	for chunkIdx := 0; chunkIdx < deviceIDCol.Len(); chunkIdx++ {
		deviceChunk := deviceIDCol.Chunk(chunkIdx)
		timeChunk := timeCol.Chunk(chunkIdx)
		latChunk := latCol.Chunk(chunkIdx)
		lonChunk := lonCol.Chunk(chunkIdx)

		chunkLen := deviceChunk.Len()

		for i := 0; i < chunkLen; i++ {
			if deviceChunk.IsNull(i) || timeChunk.IsNull(i) || latChunk.IsNull(i) || lonChunk.IsNull(i) {
				continue
			}

			rec := DeviceRecord{}

			switch arr := deviceChunk.(type) {
			case *array.String:
				rec.DeviceID = arr.Value(i)
			case *array.Binary:
				rec.DeviceID = string(arr.Value(i))
			case *array.LargeString:
				rec.DeviceID = arr.Value(i)
			}

			switch arr := timeChunk.(type) {
			case *array.Timestamp:
				rec.EventTimestamp = arr.Value(i).ToTime(arrow.Nanosecond)
			case *array.Int64:
				rec.EventTimestamp = time.Unix(arr.Value(i), 0)
			}

			switch arr := latChunk.(type) {
			case *array.Float64:
				rec.Latitude = arr.Value(i)
			case *array.Float32:
				rec.Latitude = float64(arr.Value(i))
			}

			switch arr := lonChunk.(type) {
			case *array.Float64:
				rec.Longitude = arr.Value(i)
			case *array.Float32:
				rec.Longitude = float64(arr.Value(i))
			}

			records = append(records, rec)
		}
	}

	return records
}

// Step 4: Merge campaign intersections
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

// Step 5: Filter by time
func (dt *DeviceTracker) FilterTargetTime(dateFolder string, targetDates []string, skipTimezoneError bool) error {
	fmt.Println("(DT) Started step 5")
	startTime := time.Now()

	targetFolder := filepath.Join(dt.OutputFolder, filepath.Base(dateFolder))
	os.MkdirAll(targetFolder, 0755)

	fileList, err := filepath.Glob(filepath.Join(dateFolder, "*.parquet"))
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
		var mu sync.Mutex

		jobs := make(chan string, jobBufferSize)
		var wg sync.WaitGroup

		for w := 0; w < dt.NumWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for parqFile := range jobs {
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

					if len(filtered) > 0 {
						mu.Lock()
						allFiltered = append(allFiltered, filtered...)
						mu.Unlock()
					}
				}
			}()
		}

		for _, file := range fileList {
			jobs <- file
		}
		close(jobs)
		wg.Wait()

		allFiltered = dt.deduplicateRecords(allFiltered)
		fmt.Printf("Date %s: %d records\n", targetDate, len(allFiltered))

		dt.writeDeviceCSVOptimized(outCSV, allFiltered)
	}

	fmt.Printf("(DT) Completed step 5 in %v\n", time.Since(startTime))
	return nil
}

// Step 6: Idle device search - MAIN FOCUS
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

	uniqDeviceMap := make(map[string]DeviceRecord, len(uIdDataFrame))
	for i := range uIdDataFrame {
		uniqDeviceMap[uIdDataFrame[i].DeviceID] = uIdDataFrame[i]
	}

	var allRecords []DeviceRecord
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, folderName := range folderList {
		wg.Add(1)
		go func(folder string) {
			defer wg.Done()

			csvPath := filepath.Join(dt.OutputFolder, folder, fmt.Sprintf("%s_%s.csv", dt.OutTimeFiltered, targetDate))
			if _, err := os.Stat(csvPath); os.IsNotExist(err) {
				return
			}

			records, err := dt.readDeviceCSV(csvPath)
			if err != nil {
				return
			}

			mu.Lock()
			allRecords = append(allRecords, records...)
			mu.Unlock()
		}(folderName)
	}

	wg.Wait()

	if len(allRecords) == 0 {
		return nil
	}

	deviceGroups := make(map[string][]DeviceRecord)
	for i := range allRecords {
		id := allRecords[i].DeviceID
		deviceGroups[id] = append(deviceGroups[id], allRecords[i])
	}

	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}

	idleDevices := make([]DeviceRecord, 0, len(deviceIDs)/10)
	var idleMu sync.Mutex

	jobs := make(chan string, jobBufferSize)
	var detectWg sync.WaitGroup

	for w := 0; w < dt.NumWorkers; w++ {
		detectWg.Add(1)
		go func() {
			defer detectWg.Done()

			for deviceID := range jobs {
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

					idleMu.Lock()
					idleDevices = append(idleDevices, idleRec)
					idleMu.Unlock()
				}
			}
		}()
	}

	for _, id := range deviceIDs {
		jobs <- id
	}
	close(jobs)
	detectWg.Wait()

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
		"2025-08-21", "2025-08-20", "2025-08-19", "2025-08-18",
		"2025-08-17", "2025-08-16", "2025-08-15", "2025-08-14",
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
		fmt.Println("Step 3 Completed\n")
	}

	if containsStep(runSteps, 4) {
		fmt.Println("\n========== Running STEP 4 ==========")
		err := dt.MergeCampaignIntersectionsCSV(folderList, "")
		if err != nil {
			fmt.Printf("Error in step 4: %v\n", err)
		}
		fmt.Println("Step 4 Completed\n")
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
		fmt.Println("Step 5 Completed\n")
	}

	if containsStep(runSteps, 6) {
		fmt.Println("\n========== Running STEP 6 ==========")
		err := dt.RunIdleDeviceSearch(folderList, dates)
		if err != nil {
			fmt.Printf("Error in step 6: %v\n", err)
			return err
		}
		fmt.Println("Step 6 Completed\n")
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
	fmt.Println("===========================================\n")

	startTime := time.Now()

	runSteps := []int{6}

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
