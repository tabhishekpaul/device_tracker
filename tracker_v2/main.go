package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

const (
	workerPoolSize      = 20
	clickhouseBatchSize = 3000000
)

type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// API Response structures
type APIResponse struct {
	Success bool          `json:"success"`
	Message string        `json:"message"`
	Data    []APICampaign `json:"data"`
}

type APICampaign struct {
	ID   string   `json:"_id"`
	Name string   `json:"name"`
	POIs []APIPOI `json:"pois"`
}

type APIPOI struct {
	ID      string      `json:"_id"`
	Name    string      `json:"name"`
	Polygon [][]float64 `json:"polygon"`
}

type DeviceTracker struct {
	ctx           context.Context
	FilterInTime  string
	FilterOutTime string

	filterStartHour int
	filterStartMin  int
	filterStartSec  int
	filterEndHour   int
	filterEndMin    int
	filterEndSec    int

	startSeconds int
	endSeconds   int

	TimeColumnName string
	DeviceIDColumn string
	LatColumn      string
	LonColumn      string

	CampaignAPIURL string
	LocData        []LocationRecord
	LocDataMutex   sync.RWMutex

	spatialIndex map[int][]int

	OutputFolder string

	IdleDeviceBuffer float64
	NumWorkers       int

	NTFDC atomic.Int64

	clickhouseConn *sql.DB
	chMutex        sync.Mutex
	chBatch        []TimeFilteredRecord
}

type LocationRecord struct {
	Address    string      `json:"address"`
	Campaign   string      `json:"campaign"`
	CampaignID string      `json:"campaign_id"`
	POIID      string      `json:"poi_id"`
	Geometry   orb.Polygon `json:"-"`
	Bounds     orb.Bound   `json:"-"`
}

type DeviceRecord struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	InsertDate     time.Time `json:"insert_date,omitempty"`
	Address        string    `json:"address,omitempty"`
	Campaign       string    `json:"campaign,omitempty"`
	CampaignID     string    `json:"campaign_id,omitempty"`
	POIID          string    `json:"poi_id,omitempty"`
}

// Minimal device record for JSON output (removes redundant fields)
type MinimalDeviceRecord struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
}

type TimeFilteredRecord struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	LoadDate       time.Time `json:"load_date"`
}

// Structured output for campaign-based organization
type POIDevices struct {
	POIID   string                `json:"poi_id"`
	POIName string                `json:"poi_name"`
	Devices []MinimalDeviceRecord `json:"devices"`
	Count   int                   `json:"count"`
}

type CampaignDevices struct {
	CampaignID   string       `json:"campaign_id"`
	CampaignName string       `json:"campaign_name"`
	POIs         []POIDevices `json:"pois"`
	TotalDevices int          `json:"total_devices"`
}

// Output structures for JSON files
type CampaignIntersectionOutput struct {
	ProcessedDate    string            `json:"processed_date"`
	TotalDevices     int               `json:"total_devices"`
	TotalCampaigns   int               `json:"total_campaigns"`
	ProcessingTimeMs int64             `json:"processing_time_ms"`
	Campaigns        []CampaignDevices `json:"campaigns"`
}

type MergedCampaignOutput struct {
	ProcessedDates   []string          `json:"processed_dates"`
	TotalDevices     int               `json:"total_devices"`
	UniqueDevices    int               `json:"unique_devices"`
	TotalCampaigns   int               `json:"total_campaigns"`
	ProcessingTimeMs int64             `json:"processing_time_ms"`
	Campaigns        []CampaignDevices `json:"campaigns"`
}

type IdleDevicesOutput struct {
	ProcessedDate    string            `json:"processed_date"`
	TotalIdleDevices int               `json:"total_idle_devices"`
	TotalCampaigns   int               `json:"total_campaigns"`
	BufferMeters     float64           `json:"buffer_meters"`
	ProcessingTimeMs int64             `json:"processing_time_ms"`
	Campaigns        []CampaignDevices `json:"campaigns"`
}

type FinalIdleDevicesOutput struct {
	ProcessedDates    []string          `json:"processed_dates"`
	TotalIdleDevices  int               `json:"total_idle_devices"`
	UniqueIdleDevices int               `json:"unique_idle_devices"`
	TotalCampaigns    int               `json:"total_campaigns"`
	BufferMeters      float64           `json:"buffer_meters"`
	ProcessingTimeMs  int64             `json:"processing_time_ms"`
	DevicesByCampaign map[string]int    `json:"devices_by_campaign"`
	Campaigns         []CampaignDevices `json:"campaigns"`
}

type TimeFilterOutput struct {
	ProcessedDate     string `json:"processed_date"`
	FilterStartTime   string `json:"filter_start_time"`
	FilterEndTime     string `json:"filter_end_time"`
	TotalRecords      int64  `json:"total_records"`
	FilteredInRecords int64  `json:"filtered_in_records"`
	FilteredOutCount  int64  `json:"filtered_out_count"`
	ProcessingTimeMs  int64  `json:"processing_time_ms"`
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
					t := time.Unix(0, values[i]*1000).UTC()
					result = append(result, t)
				} else {
					result = append(result, time.Time{})
				}
			}
		}
	}

	return result
}

func (dt *DeviceTracker) isWithinTimeFilter(eventTime time.Time) bool {
	hour := eventTime.Hour()
	min := eventTime.Minute()
	sec := eventTime.Second()

	eventSeconds := hour*3600 + min*60 + sec

	if dt.startSeconds < dt.endSeconds {
		return eventSeconds >= dt.startSeconds && eventSeconds < dt.endSeconds
	}

	return eventSeconds >= dt.startSeconds || eventSeconds < dt.endSeconds
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

	dt.chBatch = dt.chBatch[:0]

	return nil
}

func (dt *DeviceTracker) processCampaignFile(parqFilePath string, step3 bool, step5 bool) ([]DeviceRecord, error) {
	records, err := dt.readParquetOptimized(parqFilePath)
	if err != nil {
		return nil, err
	}

	dateStr := dt.extractDateFromPath(parqFilePath)
	insertDate, _ := time.Parse("20060102", dateStr)
	loadDate := time.Date(insertDate.Year(), insertDate.Month(), insertDate.Day(), 0, 0, 0, 0, time.UTC)

	intersectRecords := make([]DeviceRecord, 0, len(records)/10)

	dt.LocDataMutex.RLock()
	defer dt.LocDataMutex.RUnlock()

	for i := range records {
		records[i].InsertDate = insertDate

		if step5 {
			if dt.isWithinTimeFilter(records[i].EventTimestamp) {
				tfRecord := TimeFilteredRecord{
					DeviceID:       records[i].DeviceID,
					EventTimestamp: records[i].EventTimestamp,
					Latitude:       records[i].Latitude,
					Longitude:      records[i].Longitude,
					LoadDate:       loadDate,
				}

				if err := dt.addToClickHouseBatch(tfRecord); err != nil {
					fmt.Printf("Error adding to ClickHouse batch: %v\n", err)
				}
			} else {
				dt.NTFDC.Add(1)
			}
		}

		if step3 {
			point := orb.Point{records[i].Longitude, records[i].Latitude}

			candidates := dt.findIntersectingPolygons(records[i].Longitude, records[i].Latitude)

			for _, idx := range candidates {
				if !dt.LocData[idx].Bounds.Contains(point) {
					continue
				}

				if planar.PolygonContains(dt.LocData[idx].Geometry, point) {
					records[i].Address = dt.LocData[idx].Address
					records[i].Campaign = dt.LocData[idx].Campaign
					records[i].CampaignID = dt.LocData[idx].CampaignID
					records[i].POIID = dt.LocData[idx].POIID
					intersectRecords = append(intersectRecords, records[i])
					break
				}
			}
		}
	}

	log.Println("Non Time Filtered Devices Count:", dt.NTFDC.Load())
	return intersectRecords, nil
}

func NewDeviceTracker(campaignAPIURL, outputFolder string, chConfig ClickHouseConfig) (*DeviceTracker, error) {
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

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

	dt := &DeviceTracker{
		ctx:              context.Background(),
		FilterInTime:     "02:00:00",
		FilterOutTime:    "04:30:00",
		TimeColumnName:   "event_timestamp",
		DeviceIDColumn:   "device_id",
		LatColumn:        "latitude",
		LonColumn:        "longitude",
		CampaignAPIURL:   campaignAPIURL,
		OutputFolder:     outputFolder,
		IdleDeviceBuffer: 10.0,
		NumWorkers:       numWorkers,
		spatialIndex:     make(map[int][]int),
		clickhouseConn:   conn,
		chBatch:          make([]TimeFilteredRecord, 0, clickhouseBatchSize),
	}

	if err := dt.parseFilterTimes(); err != nil {
		return nil, fmt.Errorf("failed to parse filter times: %w", err)
	}

	return dt, nil
}

func (dt *DeviceTracker) parseFilterTimes() error {
	startParts := strings.Split(dt.FilterInTime, ":")
	if len(startParts) != 3 {
		return fmt.Errorf("invalid FilterInTime format: %s", dt.FilterInTime)
	}

	var err error
	dt.filterStartHour, err = strconv.Atoi(startParts[0])
	if err != nil {
		return fmt.Errorf("invalid start hour: %w", err)
	}

	dt.filterStartMin, err = strconv.Atoi(startParts[1])
	if err != nil {
		return fmt.Errorf("invalid start minute: %w", err)
	}

	dt.filterStartSec, err = strconv.Atoi(startParts[2])
	if err != nil {
		return fmt.Errorf("invalid start second: %w", err)
	}

	endParts := strings.Split(dt.FilterOutTime, ":")
	if len(endParts) != 3 {
		return fmt.Errorf("invalid FilterOutTime format: %s", dt.FilterOutTime)
	}

	dt.filterEndHour, err = strconv.Atoi(endParts[0])
	if err != nil {
		return fmt.Errorf("invalid end hour: %w", err)
	}

	dt.filterEndMin, err = strconv.Atoi(endParts[1])
	if err != nil {
		return fmt.Errorf("invalid end minute: %w", err)
	}

	dt.filterEndSec, err = strconv.Atoi(endParts[2])
	if err != nil {
		return fmt.Errorf("invalid end second: %w", err)
	}

	dt.startSeconds = dt.filterStartHour*3600 + dt.filterStartMin*60 + dt.filterStartSec
	dt.endSeconds = dt.filterEndHour*3600 + dt.filterEndMin*60 + dt.filterEndSec

	fmt.Printf("Time filter configured: %02d:%02d:%02d to %02d:%02d:%02d\n",
		dt.filterStartHour, dt.filterStartMin, dt.filterStartSec,
		dt.filterEndHour, dt.filterEndMin, dt.filterEndSec)

	return nil
}

func (dt *DeviceTracker) Close() error {
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

func (dt *DeviceTracker) addToClickHouseBatch(record TimeFilteredRecord) error {
	dt.chMutex.Lock()
	defer dt.chMutex.Unlock()

	dt.chBatch = append(dt.chBatch, record)

	if len(dt.chBatch) >= clickhouseBatchSize {
		return dt.flushClickHouseBatchUnsafe()
	}

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

func (dt *DeviceTracker) fetchCampaignsFromAPI() error {
	fmt.Printf("Fetching campaigns from API: %s\n", dt.CampaignAPIURL)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(dt.CampaignAPIURL)
	if err != nil {
		return fmt.Errorf("failed to fetch campaigns from API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned non-200 status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read API response: %w", err)
	}

	var apiResponse APIResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return fmt.Errorf("failed to parse API response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned success=false: %s", apiResponse.Message)
	}

	dt.LocDataMutex.Lock()
	defer dt.LocDataMutex.Unlock()

	dt.LocData = make([]LocationRecord, 0)

	for _, campaign := range apiResponse.Data {
		for _, poi := range campaign.POIs {
			if len(poi.Polygon) < 3 {
				fmt.Printf("Warning: POI %s has insufficient points for polygon\n", poi.Name)
				continue
			}

			ring := make(orb.Ring, 0, len(poi.Polygon))
			for _, coord := range poi.Polygon {
				if len(coord) >= 2 {
					ring = append(ring, orb.Point{coord[0], coord[1]})
				}
			}

			if len(ring) > 0 && ring[0] != ring[len(ring)-1] {
				ring = append(ring, ring[0])
			}

			polygon := orb.Polygon{ring}

			loc := LocationRecord{
				Address:    poi.Name,
				Campaign:   campaign.Name,
				CampaignID: campaign.ID,
				POIID:      poi.ID,
				Geometry:   polygon,
			}
			dt.LocData = append(dt.LocData, loc)
		}
	}

	fmt.Printf("Loaded %d location records from API (%d campaigns)\n",
		len(dt.LocData), len(apiResponse.Data))

	return nil
}

func (dt *DeviceTracker) FindCampaignIntersectionForFolder(parquetFolder string, step3 bool, step5 bool) error {
	fmt.Println("(DT) Started step 3 - Campaign Intersection")
	startTime := time.Now()

	if err := dt.fetchCampaignsFromAPI(); err != nil {
		return fmt.Errorf("failed to fetch campaigns from API: %w", err)
	}

	dt.buildSpatialIndex()

	var fileList []string

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
		fmt.Printf("No parquet files found, searching recursively in: %s\n", parquetFolder)
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

	fmt.Printf("Total Files: %d\n", len(fileList))

	// Create subfolder for this date
	dateStr := dt.extractDateFromPath(parquetFolder)
	step3Folder := filepath.Join(dt.OutputFolder, "step3_campaign_intersection", dateStr)
	os.MkdirAll(step3Folder, 0755)

	jobs := make(chan string, len(fileList))
	results := make(chan []DeviceRecord, workerPoolSize)
	var wg sync.WaitGroup

	for w := 0; w < workerPoolSize; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for parqFile := range jobs {
				records, err := dt.processCampaignFile(parqFile, step3, step5)
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

	var allRecords []DeviceRecord
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for records := range results {
			allRecords = append(allRecords, records...)
		}
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
	resultWg.Wait()

	// Remove duplicates
	seen := make(map[string]struct{})
	unique := make([]DeviceRecord, 0, len(allRecords))
	for i := range allRecords {
		if _, exists := seen[allRecords[i].DeviceID]; !exists {
			seen[allRecords[i].DeviceID] = struct{}{}
			unique = append(unique, allRecords[i])
		}
	}

	// Organize by Campaign -> POI -> Devices
	campaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	campaignNames := make(map[string]string)
	poiNames := make(map[string]map[string]string)

	for i := range unique {
		campaignID := unique[i].CampaignID
		if campaignID == "" {
			campaignID = "unknown"
		}
		poiID := unique[i].POIID
		if poiID == "" {
			poiID = "unknown"
		}

		if _, exists := campaignMap[campaignID]; !exists {
			campaignMap[campaignID] = make(map[string][]MinimalDeviceRecord)
			poiNames[campaignID] = make(map[string]string)
		}

		// Create minimal device record without redundant fields
		minimalDevice := MinimalDeviceRecord{
			DeviceID:       unique[i].DeviceID,
			EventTimestamp: unique[i].EventTimestamp,
			Latitude:       unique[i].Latitude,
			Longitude:      unique[i].Longitude,
		}

		campaignMap[campaignID][poiID] = append(campaignMap[campaignID][poiID], minimalDevice)
		campaignNames[campaignID] = unique[i].Campaign
		poiNames[campaignID][poiID] = unique[i].Address
	}

	// Convert to structured output
	campaigns := make([]CampaignDevices, 0, len(campaignMap))
	for campaignID, poisMap := range campaignMap {
		pois := make([]POIDevices, 0, len(poisMap))
		totalDevices := 0

		for poiID, devices := range poisMap {
			pois = append(pois, POIDevices{
				POIID:   poiID,
				POIName: poiNames[campaignID][poiID],
				Devices: devices,
				Count:   len(devices),
			})
			totalDevices += len(devices)
		}

		campaigns = append(campaigns, CampaignDevices{
			CampaignID:   campaignID,
			CampaignName: campaignNames[campaignID],
			POIs:         pois,
			TotalDevices: totalDevices,
		})
	}

	// Save to JSON
	output := CampaignIntersectionOutput{
		ProcessedDate:    dateStr,
		TotalDevices:     len(unique),
		TotalCampaigns:   len(campaigns),
		ProcessingTimeMs: time.Since(startTime).Milliseconds(),
		Campaigns:        campaigns,
	}

	jsonPath := filepath.Join(step3Folder, fmt.Sprintf("campaign_devices_%s.json", dateStr))
	if err := dt.saveJSON(jsonPath, output); err != nil {
		return fmt.Errorf("failed to save JSON: %w", err)
	}

	fmt.Printf("(DT) Completed step 3 in %v - Saved %d devices to %s\n",
		time.Since(startTime), len(unique), jsonPath)

	// Also save time filter stats if step5 was run
	if step5 {
		step5Folder := filepath.Join(dt.OutputFolder, "step5_time_filtered", dateStr)
		os.MkdirAll(step5Folder, 0755)

		timeFilterStats := TimeFilterOutput{
			ProcessedDate:     dateStr,
			FilterStartTime:   dt.FilterInTime,
			FilterEndTime:     dt.FilterOutTime,
			TotalRecords:      int64(len(allRecords)),
			FilteredInRecords: int64(len(allRecords)) - dt.NTFDC.Load(),
			FilteredOutCount:  dt.NTFDC.Load(),
			ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		}

		statsPath := filepath.Join(step5Folder, fmt.Sprintf("time_filter_stats_%s.json", dateStr))
		if err := dt.saveJSON(statsPath, timeFilterStats); err != nil {
			fmt.Printf("Warning: failed to save time filter stats: %v\n", err)
		}
	}

	return nil
}

func (dt *DeviceTracker) MergeCampaignIntersectionsJSON(folderList []string) error {
	fmt.Println("(DT) Started step 4 - Merge Campaign Intersections")
	startTime := time.Now()

	step4Folder := filepath.Join(dt.OutputFolder, "step4_merged_campaigns")
	os.MkdirAll(step4Folder, 0755)

	type result struct {
		campaigns []CampaignDevices
		date      string
		err       error
	}

	results := make(chan result, len(folderList))
	var wg sync.WaitGroup

	for _, folder := range folderList {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()

			dateStr := strings.TrimPrefix(f, "load_date=")
			step3Folder := filepath.Join(dt.OutputFolder, "step3_campaign_intersection", dateStr)
			jsonPath := filepath.Join(step3Folder, fmt.Sprintf("campaign_devices_%s.json", dateStr))

			var output CampaignIntersectionOutput
			if err := dt.loadJSON(jsonPath, &output); err != nil {
				results <- result{nil, dateStr, err}
				return
			}

			results <- result{output.Campaigns, dateStr, nil}
		}(folder)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect all campaigns
	allCampaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	allCampaignNames := make(map[string]string)
	allPOINames := make(map[string]map[string]string)
	var processedDates []string
	totalDeviceCount := 0

	for res := range results {
		if res.err != nil {
			fmt.Printf("Warning: failed to load data for date %s: %v\n", res.date, res.err)
			continue
		}

		processedDates = append(processedDates, res.date)

		for _, campaign := range res.campaigns {
			if _, exists := allCampaignMap[campaign.CampaignID]; !exists {
				allCampaignMap[campaign.CampaignID] = make(map[string][]MinimalDeviceRecord)
				allPOINames[campaign.CampaignID] = make(map[string]string)
			}
			allCampaignNames[campaign.CampaignID] = campaign.CampaignName

			for _, poi := range campaign.POIs {
				allCampaignMap[campaign.CampaignID][poi.POIID] = append(
					allCampaignMap[campaign.CampaignID][poi.POIID],
					poi.Devices...,
				)
				allPOINames[campaign.CampaignID][poi.POIID] = poi.POIName
				totalDeviceCount += len(poi.Devices)
			}
		}
	}

	// Convert to structured output for all devices
	allCampaigns := make([]CampaignDevices, 0, len(allCampaignMap))
	for campaignID, poisMap := range allCampaignMap {
		pois := make([]POIDevices, 0, len(poisMap))
		totalDevices := 0

		for poiID, devices := range poisMap {
			pois = append(pois, POIDevices{
				POIID:   poiID,
				POIName: allPOINames[campaignID][poiID],
				Devices: devices,
				Count:   len(devices),
			})
			totalDevices += len(devices)
		}

		allCampaigns = append(allCampaigns, CampaignDevices{
			CampaignID:   campaignID,
			CampaignName: allCampaignNames[campaignID],
			POIs:         pois,
			TotalDevices: totalDevices,
		})
	}

	// Save all devices
	allOutput := MergedCampaignOutput{
		ProcessedDates:   processedDates,
		TotalDevices:     totalDeviceCount,
		UniqueDevices:    0,
		TotalCampaigns:   len(allCampaigns),
		ProcessingTimeMs: time.Since(startTime).Milliseconds(),
		Campaigns:        allCampaigns,
	}

	allPath := filepath.Join(step4Folder, "all_campaign_devices.json")
	if err := dt.saveJSON(allPath, allOutput); err != nil {
		return fmt.Errorf("failed to save all devices: %w", err)
	}

	// Create unique devices list
	uniqueCampaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	uniqueCampaignNames := make(map[string]string)
	uniquePOINames := make(map[string]map[string]string)
	seen := make(map[string]struct{})
	uniqueCount := 0

	for campaignID, poisMap := range allCampaignMap {
		if _, exists := uniqueCampaignMap[campaignID]; !exists {
			uniqueCampaignMap[campaignID] = make(map[string][]MinimalDeviceRecord)
			uniquePOINames[campaignID] = make(map[string]string)
		}
		uniqueCampaignNames[campaignID] = allCampaignNames[campaignID]

		for poiID, devices := range poisMap {
			uniquePOINames[campaignID][poiID] = allPOINames[campaignID][poiID]

			for i := range devices {
				if _, exists := seen[devices[i].DeviceID]; !exists {
					seen[devices[i].DeviceID] = struct{}{}
					uniqueCampaignMap[campaignID][poiID] = append(
						uniqueCampaignMap[campaignID][poiID],
						devices[i],
					)
					uniqueCount++
				}
			}
		}
	}

	// Convert to structured output for unique devices
	uniqueCampaigns := make([]CampaignDevices, 0, len(uniqueCampaignMap))
	for campaignID, poisMap := range uniqueCampaignMap {
		pois := make([]POIDevices, 0, len(poisMap))
		totalDevices := 0

		for poiID, devices := range poisMap {
			if len(devices) > 0 {
				pois = append(pois, POIDevices{
					POIID:   poiID,
					POIName: uniquePOINames[campaignID][poiID],
					Devices: devices,
					Count:   len(devices),
				})
				totalDevices += len(devices)
			}
		}

		if totalDevices > 0 {
			uniqueCampaigns = append(uniqueCampaigns, CampaignDevices{
				CampaignID:   campaignID,
				CampaignName: uniqueCampaignNames[campaignID],
				POIs:         pois,
				TotalDevices: totalDevices,
			})
		}
	}

	uniqueOutput := MergedCampaignOutput{
		ProcessedDates:   processedDates,
		TotalDevices:     totalDeviceCount,
		UniqueDevices:    uniqueCount,
		TotalCampaigns:   len(uniqueCampaigns),
		ProcessingTimeMs: time.Since(startTime).Milliseconds(),
		Campaigns:        uniqueCampaigns,
	}

	uniquePath := filepath.Join(step4Folder, "unique_campaign_devices.json")
	if err := dt.saveJSON(uniquePath, uniqueOutput); err != nil {
		return fmt.Errorf("failed to save unique devices: %w", err)
	}

	fmt.Printf("(DT) Completed step 4 in %v\n", time.Since(startTime))
	fmt.Printf("  Total devices: %d\n", totalDeviceCount)
	fmt.Printf("  Unique devices: %d\n", uniqueCount)
	fmt.Printf("  Saved to: %s\n", step4Folder)

	return nil
}

func (dt *DeviceTracker) RunIdleDeviceSearch(targetDates []string) error {
	fmt.Println("(DT) Started step 6 - Idle Device Search")
	startTime := time.Now()

	step6Folder := filepath.Join(dt.OutputFolder, "step6_idle_devices")
	os.MkdirAll(step6Folder, 0755)

	for _, date := range targetDates {
		fmt.Printf("Processing date: %s\n", date)

		err := dt.findIdleDevicesFromClickHouse(date, step6Folder)
		if err != nil {
			fmt.Printf("Error for %s: %v\n", date, err)
		}
	}

	err := dt.mergeIdleDevicesOutput(targetDates, step6Folder)
	if err != nil {
		return err
	}

	fmt.Printf("(DT) Completed step 6 in %v\n", time.Since(startTime))
	return nil
}

func (dt *DeviceTracker) findIdleDevicesFromClickHouse(targetDate string, outputFolder string) error {
	stepStartTime := time.Now()

	// Load unique device data from merged campaign JSON
	step4Folder := filepath.Join(dt.OutputFolder, "step4_merged_campaigns")
	uniquePath := filepath.Join(step4Folder, "unique_campaign_devices.json")

	var mergedOutput MergedCampaignOutput
	if err := dt.loadJSON(uniquePath, &mergedOutput); err != nil {
		return fmt.Errorf("failed to load merged campaign devices: %w", err)
	}

	// Extract all devices from the campaigns structure and build a map
	uniqDeviceMap := make(map[string]DeviceRecord)
	for _, campaign := range mergedOutput.Campaigns {
		for _, poi := range campaign.POIs {
			for _, device := range poi.Devices {
				// Reconstruct full DeviceRecord with campaign and POI info
				fullDevice := DeviceRecord{
					DeviceID:       device.DeviceID,
					EventTimestamp: device.EventTimestamp,
					Latitude:       device.Latitude,
					Longitude:      device.Longitude,
					Campaign:       campaign.CampaignName,
					CampaignID:     campaign.CampaignID,
					POIID:          poi.POIID,
					Address:        poi.POIName,
				}
				uniqDeviceMap[device.DeviceID] = fullDevice
			}
		}
	}

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

	deviceIDs := make([]string, 0, len(deviceGroups))
	for id := range deviceGroups {
		deviceIDs = append(deviceIDs, id)
	}

	type idleDevice struct {
		DeviceID   string
		CampaignID string
		POIID      string
		Campaign   string
		Address    string
		Latitude   float64
		Longitude  float64
		VisitTime  time.Time
	}

	idleDevicesChan := make(chan idleDevice, len(deviceIDs))
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
					idleDevicesChan <- idleDevice{
						DeviceID:   deviceID,
						CampaignID: campRec.CampaignID,
						POIID:      campRec.POIID,
						Campaign:   campRec.Campaign,
						Address:    campRec.Address,
						Latitude:   firstRec.Latitude,
						Longitude:  firstRec.Longitude,
						VisitTime:  campRec.EventTimestamp,
					}
				}
			}
		}(deviceIDs[start:end])
	}

	go func() {
		processWg.Wait()
		close(idleDevicesChan)
	}()

	idleDevices := make([]idleDevice, 0)
	for rec := range idleDevicesChan {
		idleDevices = append(idleDevices, rec)
	}

	// Remove duplicates
	seen := make(map[string]struct{})
	unique := make([]idleDevice, 0, len(idleDevices))
	for i := range idleDevices {
		if _, exists := seen[idleDevices[i].DeviceID]; !exists {
			seen[idleDevices[i].DeviceID] = struct{}{}
			unique = append(unique, idleDevices[i])
		}
	}

	// Organize by Campaign -> POI -> Devices
	campaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	campaignNames := make(map[string]string)
	poiNames := make(map[string]map[string]string)

	for i := range unique {
		campaignID := unique[i].CampaignID
		if campaignID == "" {
			campaignID = "unknown"
		}
		poiID := unique[i].POIID
		if poiID == "" {
			poiID = "unknown"
		}

		if _, exists := campaignMap[campaignID]; !exists {
			campaignMap[campaignID] = make(map[string][]MinimalDeviceRecord)
			poiNames[campaignID] = make(map[string]string)
		}

		minimalDevice := MinimalDeviceRecord{
			DeviceID:       unique[i].DeviceID,
			EventTimestamp: unique[i].VisitTime,
			Latitude:       unique[i].Latitude,
			Longitude:      unique[i].Longitude,
		}

		campaignMap[campaignID][poiID] = append(campaignMap[campaignID][poiID], minimalDevice)
		campaignNames[campaignID] = unique[i].Campaign
		poiNames[campaignID][poiID] = unique[i].Address
	}

	// Convert to structured output
	campaigns := make([]CampaignDevices, 0, len(campaignMap))
	for campaignID, poisMap := range campaignMap {
		pois := make([]POIDevices, 0, len(poisMap))
		totalDevices := 0

		for poiID, devices := range poisMap {
			pois = append(pois, POIDevices{
				POIID:   poiID,
				POIName: poiNames[campaignID][poiID],
				Devices: devices,
				Count:   len(devices),
			})
			totalDevices += len(devices)
		}

		campaigns = append(campaigns, CampaignDevices{
			CampaignID:   campaignID,
			CampaignName: campaignNames[campaignID],
			POIs:         pois,
			TotalDevices: totalDevices,
		})
	}

	output := IdleDevicesOutput{
		ProcessedDate:    targetDate,
		TotalIdleDevices: len(unique),
		TotalCampaigns:   len(campaigns),
		BufferMeters:     dt.IdleDeviceBuffer,
		ProcessingTimeMs: time.Since(stepStartTime).Milliseconds(),
		Campaigns:        campaigns,
	}

	jsonPath := filepath.Join(outputFolder, fmt.Sprintf("idle_devices_%s.json", strings.ReplaceAll(targetDate, "-", "")))
	if err := dt.saveJSON(jsonPath, output); err != nil {
		return fmt.Errorf("failed to save idle devices: %w", err)
	}

	fmt.Printf("Idle devices found: %d - Saved to %s\n", len(unique), jsonPath)
	return nil
}

func (dt *DeviceTracker) mergeIdleDevicesOutput(targetDates []string, step6Folder string) error {
	fmt.Println("Merging all idle device results...")
	startTime := time.Now()

	allCampaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	allCampaignNames := make(map[string]string)
	allPOINames := make(map[string]map[string]string)
	var processedDates []string

	for _, targetDate := range targetDates {
		dateStr := strings.ReplaceAll(targetDate, "-", "")
		jsonPath := filepath.Join(step6Folder, fmt.Sprintf("idle_devices_%s.json", dateStr))

		var output IdleDevicesOutput
		if err := dt.loadJSON(jsonPath, &output); err != nil {
			fmt.Printf("Warning: failed to load %s: %v\n", jsonPath, err)
			continue
		}

		processedDates = append(processedDates, targetDate)

		// Merge campaigns
		for _, campaign := range output.Campaigns {
			if _, exists := allCampaignMap[campaign.CampaignID]; !exists {
				allCampaignMap[campaign.CampaignID] = make(map[string][]MinimalDeviceRecord)
				allPOINames[campaign.CampaignID] = make(map[string]string)
			}
			allCampaignNames[campaign.CampaignID] = campaign.CampaignName

			for _, poi := range campaign.POIs {
				allCampaignMap[campaign.CampaignID][poi.POIID] = append(
					allCampaignMap[campaign.CampaignID][poi.POIID],
					poi.Devices...,
				)
				allPOINames[campaign.CampaignID][poi.POIID] = poi.POIName
			}
		}
	}

	if len(allCampaignMap) == 0 {
		fmt.Println("No idle devices found to merge")
		return nil
	}

	// Create unique list by device_id + campaign
	seen := make(map[string]struct{})
	uniqueCampaignMap := make(map[string]map[string][]MinimalDeviceRecord)
	campaignCount := make(map[string]int)
	totalCount := 0
	uniqueCount := 0

	for campaignID, poisMap := range allCampaignMap {
		if _, exists := uniqueCampaignMap[campaignID]; !exists {
			uniqueCampaignMap[campaignID] = make(map[string][]MinimalDeviceRecord)
		}

		for poiID, devices := range poisMap {
			totalCount += len(devices)

			for i := range devices {
				key := devices[i].DeviceID + "|" + campaignID
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					uniqueCampaignMap[campaignID][poiID] = append(
						uniqueCampaignMap[campaignID][poiID],
						devices[i],
					)
					campaignCount[allCampaignNames[campaignID]]++
					uniqueCount++
				}
			}
		}
	}

	// Convert to structured output
	campaigns := make([]CampaignDevices, 0, len(uniqueCampaignMap))
	for campaignID, poisMap := range uniqueCampaignMap {
		pois := make([]POIDevices, 0, len(poisMap))
		totalDevices := 0

		for poiID, devices := range poisMap {
			if len(devices) > 0 {
				pois = append(pois, POIDevices{
					POIID:   poiID,
					POIName: allPOINames[campaignID][poiID],
					Devices: devices,
					Count:   len(devices),
				})
				totalDevices += len(devices)
			}
		}

		if totalDevices > 0 {
			campaigns = append(campaigns, CampaignDevices{
				CampaignID:   campaignID,
				CampaignName: allCampaignNames[campaignID],
				POIs:         pois,
				TotalDevices: totalDevices,
			})
		}
	}

	output := FinalIdleDevicesOutput{
		ProcessedDates:    processedDates,
		TotalIdleDevices:  totalCount,
		UniqueIdleDevices: uniqueCount,
		TotalCampaigns:    len(campaigns),
		BufferMeters:      dt.IdleDeviceBuffer,
		ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		DevicesByCampaign: campaignCount,
		Campaigns:         campaigns,
	}

	finalPath := filepath.Join(step6Folder, "final_idle_devices.json")
	if err := dt.saveJSON(finalPath, output); err != nil {
		return fmt.Errorf("failed to save final idle devices: %w", err)
	}

	fmt.Printf("Final idle devices saved to: %s\n", finalPath)
	fmt.Printf("  Total idle devices: %d\n", totalCount)
	fmt.Printf("  Unique idle devices: %d\n", uniqueCount)
	fmt.Printf("  Campaigns: %d\n", len(campaignCount))

	return nil
}

func (dt *DeviceTracker) saveJSON(filePath string, data interface{}) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func (dt *DeviceTracker) loadJSON(filePath string, data interface{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(data)
}

func (dt *DeviceTracker) readParquetOptimized(filePath string) ([]DeviceRecord, error) {
	pf, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	return dt.readParquetFromFile(pf)
}

func (dt *DeviceTracker) readParquetFromFile(pf *file.Reader) ([]DeviceRecord, error) {
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

func RunDeviceTracker(skipTimezoneError bool, runForPastDays bool, runSteps []int) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	dates := []string{
		"2025-10-13",
		"2025-10-14",
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
		"https://locatrix-backend-development.up.railway.app/api/admin/activecampaign/list",
		"/home/device-tracker/data/output",
		chConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to create device tracker: %w", err)
	}
	defer dt.Close()

	step3 := containsStep(runSteps, 3)
	step5 := containsStep(runSteps, 5)

	if step3 || step5 {
		fmt.Println("\n========== Running STEP 3 & 5 ==========")
		for _, folder := range folderList {
			err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/"+folder, step3, step5)
			if err != nil {
				fmt.Printf("Error in step 3/5 for %s: %v\n", folder, err)
			}
		}
		fmt.Println("Step 3 & 5 Completed")
	}

	if containsStep(runSteps, 4) {
		fmt.Println("\n========== Running STEP 4 ==========")
		err := dt.MergeCampaignIntersectionsJSON(folderList)
		if err != nil {
			fmt.Printf("Error in step 4: %v\n", err)
		}
		fmt.Println("Step 4 Completed")
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
	fmt.Println("   Device Tracker - JSON Output Format")
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
