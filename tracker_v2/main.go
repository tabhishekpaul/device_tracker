package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/planar"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	parquetBatchSize = 1000000
)

type MongoConfig struct {
	URI        string
	Database   string
	Collection string
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

	// Parquet batch writer fields
	parquetMutex  sync.Mutex
	parquetBatch  []TimeFilteredRecord
	parquetWriter *pqarrow.FileWriter
	parquetFile   *os.File
	parquetSchema *arrow.Schema
	currentDate   string

	mongoClient     *mongo.Client
	mongoCollection *mongo.Collection
	mongoConfig     MongoConfig
}

type LocationRecord struct {
	Address    string                `json:"address"`
	Campaign   string                `json:"campaign"`
	CampaignID string                `json:"campaign_id"`
	POIID      string                `json:"poi_id"`
	Geometry   orb.Polygon           `json:"-"`
	Bounds     orb.Bound             `json:"-"`
	Devices    []MinimalDeviceRecord `bson:"devices"`
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

// Minimal device record for JSON output
type MinimalDeviceRecord struct {
	DeviceID       string    `json:"device_id" bson:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp" bson:"event_timestamp"`
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

// MongoDB document structure
type POIMongoDocument struct {
	ID            primitive.ObjectID    `bson:"_id,omitempty"`
	POIID         primitive.ObjectID    `bson:"poi_id"`
	POIName       string                `bson:"poi_name"`
	ProcessedDate string                `bson:"processed_date"`
	DeviceCount   int                   `bson:"device_count"`
	Devices       []MinimalDeviceRecord `bson:"devices"`
	CreatedAt     time.Time             `bson:"created_at"`
}

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

type TimeFilterOutput struct {
	ProcessedDate     string `json:"processed_date"`
	FilterStartTime   string `json:"filter_start_time"`
	FilterEndTime     string `json:"filter_end_time"`
	TotalRecords      int64  `json:"total_records"`
	FilteredInRecords int64  `json:"filtered_in_records"`
	FilteredOutCount  int64  `json:"filtered_out_count"`
	ProcessingTimeMs  int64  `json:"processing_time_ms"`
}

// Step 3 structures
type CampaignMetadata struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Address        string    `json:"address"`
	Campaign       string    `json:"campaign"`
	CampaignID     string    `json:"campaign_id"`
	POIID          string    `json:"poi_id"`
}

type IdleDeviceResult struct {
	DeviceID    string `json:"device_id"`
	VisitedTime string `json:"visited_time"`
	Address     string `json:"address"`
	Campaign    string `json:"campaign"`
	CampaignID  string `json:"campaign_id"`
	POIID       string `json:"poi_id"`
	Geometry    string `json:"geometry"`
}

type DeviceTrackingState struct {
	FirstPoint  orb.Point
	FirstTime   time.Time
	IsStillIdle bool
	RecordCount int
}

type IdleDevicesByDate struct {
	EventDate        string             `json:"event_date"`
	TotalIdleDevices int                `json:"total_idle_devices"`
	IdleDevices      []IdleDeviceResult `json:"idle_devices"`
}

type FinalIdleDevicesOutput struct {
	ProcessedDates    []string                      `json:"processed_dates"`
	TotalIdleDevices  int                           `json:"total_idle_devices"`
	ProcessingTimeMs  int64                         `json:"processing_time_ms"`
	IdleDevicesByDate map[string][]IdleDeviceResult `json:"idle_devices_by_date"`
}

// ============================================================================
// CONSTRUCTOR
// ============================================================================

func NewDeviceTracker(campaignAPIURL, outputFolder string, mongoConfig MongoConfig) (*DeviceTracker, error) {
	ctx := context.Background()

	dt := &DeviceTracker{
		ctx:              ctx,
		FilterInTime:     "02:00:00",
		FilterOutTime:    "04:30:00",
		TimeColumnName:   "event_timestamp",
		DeviceIDColumn:   "device_id",
		LatColumn:        "latitude",
		LonColumn:        "longitude",
		CampaignAPIURL:   campaignAPIURL,
		OutputFolder:     outputFolder,
		IdleDeviceBuffer: 10.0,
		NumWorkers:       8,
		mongoConfig:      mongoConfig,
	}

	// Parse time filters
	dt.parseTimeFilters()

	// Connect to MongoDB
	if err := dt.connectMongoDB(); err != nil {
		return nil, err
	}

	return dt, nil
}

func (dt *DeviceTracker) parseTimeFilters() {
	fmt.Sscanf(dt.FilterInTime, "%d:%d:%d", &dt.filterStartHour, &dt.filterStartMin, &dt.filterStartSec)
	fmt.Sscanf(dt.FilterOutTime, "%d:%d:%d", &dt.filterEndHour, &dt.filterEndMin, &dt.filterEndSec)

	dt.startSeconds = dt.filterStartHour*3600 + dt.filterStartMin*60 + dt.filterStartSec
	dt.endSeconds = dt.filterEndHour*3600 + dt.filterEndMin*60 + dt.filterEndSec

	fmt.Printf("Time filter configured: %s to %s\n", dt.FilterInTime, dt.FilterOutTime)
}

func (dt *DeviceTracker) connectMongoDB() error {
	clientOptions := options.Client().ApplyURI(dt.mongoConfig.URI)
	client, err := mongo.Connect(dt.ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(dt.ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	dt.mongoClient = client
	dt.mongoCollection = client.Database(dt.mongoConfig.Database).Collection(dt.mongoConfig.Collection)

	fmt.Println("✅ Successfully connected to MongoDB")
	return nil
}

func (dt *DeviceTracker) Close() {
	if dt.mongoClient != nil {
		dt.mongoClient.Disconnect(dt.ctx)
	}
}

// ============================================================================
// API FETCHING
// ============================================================================

func (dt *DeviceTracker) fetchCampaignsFromAPI() error {
	fmt.Printf("Fetching campaigns from API: %s\n", dt.CampaignAPIURL)

	resp, err := http.Get(dt.CampaignAPIURL)
	if err != nil {
		return fmt.Errorf("failed to fetch campaigns: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if !apiResp.Success {
		return fmt.Errorf("API returned error: %s", apiResp.Message)
	}

	dt.LocDataMutex.Lock()
	defer dt.LocDataMutex.Unlock()

	dt.LocData = make([]LocationRecord, 0)

	for _, campaign := range apiResp.Data {
		for _, poi := range campaign.POIs {
			if len(poi.Polygon) == 0 {
				continue
			}

			ring := make(orb.Ring, 0, len(poi.Polygon))
			for _, coord := range poi.Polygon {
				if len(coord) >= 2 {
					ring = append(ring, orb.Point{coord[0], coord[1]})
				}
			}

			if len(ring) > 0 && !ring[0].Equal(ring[len(ring)-1]) {
				ring = append(ring, ring[0])
			}

			polygon := orb.Polygon{ring}
			bounds := polygon.Bound()

			dt.LocData = append(dt.LocData, LocationRecord{
				Address:    poi.Name,
				Campaign:   campaign.Name,
				CampaignID: campaign.ID,
				POIID:      poi.ID,
				Geometry:   polygon,
				Bounds:     bounds,
			})
		}
	}

	fmt.Printf("Loaded %d location records from API (%d campaigns)\n", len(dt.LocData), len(apiResp.Data))
	return nil
}

// ============================================================================
// SPATIAL INDEX
// ============================================================================

func (dt *DeviceTracker) buildSpatialIndex() {
	dt.spatialIndex = make(map[int][]int)

	const gridSize = 0.01

	for i, loc := range dt.LocData {
		minX := int(loc.Bounds.Min[0] / gridSize)
		maxX := int(loc.Bounds.Max[0] / gridSize)
		minY := int(loc.Bounds.Min[1] / gridSize)
		maxY := int(loc.Bounds.Max[1] / gridSize)

		for x := minX; x <= maxX; x++ {
			for y := minY; y <= maxY; y++ {
				cellKey := x*10000 + y
				dt.spatialIndex[cellKey] = append(dt.spatialIndex[cellKey], i)
			}
		}
	}

	fmt.Printf("Built spatial index with %d cells\n", len(dt.spatialIndex))
}

func (dt *DeviceTracker) findIntersectingLocations(point orb.Point) []int {
	const gridSize = 0.01
	cellX := int(point[0] / gridSize)
	cellY := int(point[1] / gridSize)
	cellKey := cellX*10000 + cellY

	candidates, exists := dt.spatialIndex[cellKey]
	if !exists {
		return nil
	}

	results := make([]int, 0)
	for _, idx := range candidates {
		loc := dt.LocData[idx]
		if planar.PolygonContains(loc.Geometry, point) {
			results = append(results, idx)
		}
	}

	return results
}

// ============================================================================
// STEP 1: CAMPAIGN INTERSECTION
// ============================================================================

func (dt *DeviceTracker) FindCampaignIntersectionForFolder(folderPath string, runStep1, runStep2 bool) error {
	dateStr := dt.extractDateFromPath(folderPath)
	if dateStr == "" {
		return fmt.Errorf("could not extract date from path: %s", folderPath)
	}

	// STEP 1: Campaign Intersection
	if runStep1 {
		fmt.Printf("\n🔍 STEP 1: Campaign Intersection for %s\n", dateStr)

		parquetFiles, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
		if err != nil {
			return fmt.Errorf("failed to find parquet files: %w", err)
		}

		fmt.Printf("  📂 Found %d parquet files\n", len(parquetFiles))

		deviceMap := make(map[string]map[string]MinimalDeviceRecord)
		var mu sync.Mutex

		for idx, parquetFile := range parquetFiles {
			if idx%10 == 0 {
				fmt.Printf("    🔄 Processing file %d/%d...\n", idx+1, len(parquetFiles))
			}

			pf, err := file.OpenParquetFile(parquetFile, false)
			if err != nil {
				continue
			}

			numRowGroups := pf.NumRowGroups()
			for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
				records, err := dt.readRowGroup(pf, rgIdx)
				if err != nil {
					continue
				}

				for _, record := range records {
					point := orb.Point{record.Longitude, record.Latitude}
					locIndices := dt.findIntersectingLocations(point)

					if len(locIndices) > 0 {
						mu.Lock()
						for _, locIdx := range locIndices {
							loc := dt.LocData[locIdx]
							key := fmt.Sprintf("%s_%s_%s", loc.CampaignID, loc.POIID, record.DeviceID)

							if _, exists := deviceMap[key]; !exists {
								deviceMap[key] = map[string]MinimalDeviceRecord{
									"device": {
										DeviceID:       record.DeviceID,
										EventTimestamp: record.EventTimestamp,
									},
									"loc": {
										DeviceID: fmt.Sprintf("%s|%s|%s|%s", loc.Address, loc.Campaign, loc.CampaignID, loc.POIID),
									},
								}
							}
						}
						mu.Unlock()
					}
				}
			}

			pf.Close()
			runtime.GC()
		}

		// Save to JSON
		err = dt.saveCampaignIntersectionJSON(deviceMap, dateStr)
		if err != nil {
			return fmt.Errorf("failed to save campaign intersection: %w", err)
		}

		fmt.Printf("  ✅ Step 1 completed: %d unique devices in campaigns\n", len(deviceMap))
	}

	// STEP 2: Time Filtering WITH CAMPAIGN FILTER
	if runStep2 {
		fmt.Printf("\n⏰ STEP 2: Time Filtering for %s\n", dateStr)

		// Load campaign device set from Step 1
		campaignDeviceSet, err := dt.loadCampaignDeviceSetFromJSON(dateStr)
		if err != nil {
			return fmt.Errorf("step 2 requires Step 1: %w", err)
		}

		fmt.Printf("  📋 Filtering for %d campaign devices\n", len(campaignDeviceSet))

		dt.NTFDC.Store(0)

		err = dt.initParquetWriter(dateStr)
		if err != nil {
			return fmt.Errorf("failed to init parquet writer: %w", err)
		}

		parquetFiles, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
		if err != nil {
			dt.closeParquetWriter()
			return fmt.Errorf("failed to find parquet files: %w", err)
		}

		fmt.Printf("  📂 Processing %d parquet files...\n", len(parquetFiles))

		for idx, parquetFile := range parquetFiles {
			if idx%10 == 0 {
				fmt.Printf("    🔄 File %d/%d...\n", idx+1, len(parquetFiles))
			}

			pf, err := file.OpenParquetFile(parquetFile, false)
			if err != nil {
				continue
			}

			numRowGroups := pf.NumRowGroups()
			for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
				dt.processRowGroupWithTimeFilter(pf, rgIdx, campaignDeviceSet)
			}

			pf.Close()
			runtime.GC()
		}

		dt.parquetMutex.Lock()
		dt.flushParquetBatchUnsafe()
		dt.parquetMutex.Unlock()

		dt.closeParquetWriter()

		fmt.Printf("  ✅ Step 2 completed: %d records passed filters\n", dt.NTFDC.Load())
	}

	return nil
}

// loadCampaignDeviceSetFromJSON - CRITICAL FIX for Step 2
func (dt *DeviceTracker) loadCampaignDeviceSetFromJSON(dateStr string) (map[string]bool, error) {
	jsonPath := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr,
		fmt.Sprintf("campaign_devices_%s.json", dateStr))

	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("campaign JSON not found: %s (run Step 1 first)", jsonPath)
	}

	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var campaignOutput CampaignIntersectionOutput
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&campaignOutput); err != nil {
		return nil, err
	}

	deviceSet := make(map[string]bool, 100000)
	for _, campaign := range campaignOutput.Campaigns {
		for _, poi := range campaign.POIs {
			for _, device := range poi.Devices {
				deviceSet[device.DeviceID] = true
			}
		}
	}

	return deviceSet, nil
}

// processRowGroupWithTimeFilter - MODIFIED with campaign filter
func (dt *DeviceTracker) processRowGroupWithTimeFilter(pf *file.Reader, rgIdx int, campaignDeviceSet map[string]bool) error {
	records, err := dt.readRowGroup(pf, rgIdx)
	if err != nil {
		return err
	}

	for i := range records {
		record := &records[i]

		// CRITICAL: Filter by campaign devices first
		if !campaignDeviceSet[record.DeviceID] {
			continue
		}

		// Then check time filter
		if dt.isWithinTimeFilter(record.EventTimestamp) {
			dt.parquetMutex.Lock()
			dt.parquetBatch = append(dt.parquetBatch, TimeFilteredRecord{
				DeviceID:       record.DeviceID,
				EventTimestamp: record.EventTimestamp,
				Latitude:       record.Latitude,
				Longitude:      record.Longitude,
				LoadDate:       time.Now(),
			})

			if len(dt.parquetBatch) >= parquetBatchSize {
				dt.flushParquetBatchUnsafe()
			}
			dt.parquetMutex.Unlock()

			dt.NTFDC.Add(1)
		}
	}

	return nil
}

func (dt *DeviceTracker) saveCampaignIntersectionJSON(deviceMap map[string]map[string]MinimalDeviceRecord, dateStr string) error {
	// Group by campaign and POI
	campaignMap := make(map[string]map[string][]MinimalDeviceRecord)

	for _, data := range deviceMap {
		deviceRecord := data["device"]
		locData := data["loc"].DeviceID

		parts := strings.Split(locData, "|")
		if len(parts) != 4 {
			continue
		}

		address, campaign, campaignID, poiID := parts[0], parts[1], parts[2], parts[3]

		if _, exists := campaignMap[campaignID]; !exists {
			campaignMap[campaignID] = make(map[string][]MinimalDeviceRecord)
		}

		key := fmt.Sprintf("%s|%s|%s", poiID, address, campaign)
		campaignMap[campaignID][key] = append(campaignMap[campaignID][key], deviceRecord)
	}

	// Build output structure
	campaigns := make([]CampaignDevices, 0)
	totalDevices := 0

	for campaignID, poiMap := range campaignMap {
		pois := make([]POIDevices, 0)
		campaignDeviceCount := 0
		var campaignName string

		for key, devices := range poiMap {
			parts := strings.Split(key, "|")
			poiID, poiName, campName := parts[0], parts[1], parts[2]
			campaignName = campName

			pois = append(pois, POIDevices{
				POIID:   poiID,
				POIName: poiName,
				Devices: devices,
				Count:   len(devices),
			})

			campaignDeviceCount += len(devices)
		}

		campaigns = append(campaigns, CampaignDevices{
			CampaignID:   campaignID,
			CampaignName: campaignName,
			POIs:         pois,
			TotalDevices: campaignDeviceCount,
		})

		totalDevices += campaignDeviceCount
	}

	output := CampaignIntersectionOutput{
		ProcessedDate:    dateStr,
		TotalDevices:     totalDevices,
		TotalCampaigns:   len(campaigns),
		ProcessingTimeMs: 0,
		Campaigns:        campaigns,
	}

	// Save JSON
	outputDir := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr)
	os.MkdirAll(outputDir, 0755)

	jsonPath := filepath.Join(outputDir, fmt.Sprintf("campaign_devices_%s.json", dateStr))
	file, err := os.Create(jsonPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

// ============================================================================
// PARQUET OPERATIONS
// ============================================================================

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

func (dt *DeviceTracker) initParquetWriter(dateStr string) error {
	dt.parquetMutex.Lock()
	defer dt.parquetMutex.Unlock()

	if dt.parquetWriter != nil && dt.currentDate == dateStr {
		return nil
	}

	if dt.parquetWriter != nil {
		dt.closeParquetWriterUnsafe()
	}

	timeFilteredFolder := filepath.Join(dt.OutputFolder, "time_filtered", dateStr)
	os.MkdirAll(timeFilteredFolder, 0755)

	parquetPath := filepath.Join(timeFilteredFolder, fmt.Sprintf("time_filtered_loaddate%s.parquet", dateStr))

	file, err := os.Create(parquetPath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "device_id", Type: arrow.BinaryTypes.String},
			{Name: "event_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
			{Name: "latitude", Type: arrow.PrimitiveTypes.Float64},
			{Name: "longitude", Type: arrow.PrimitiveTypes.Float64},
			{Name: "load_date", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		},
		nil,
	)

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)

	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	dt.parquetWriter = writer
	dt.parquetFile = file
	dt.parquetSchema = schema
	dt.currentDate = dateStr
	dt.parquetBatch = make([]TimeFilteredRecord, 0, parquetBatchSize)

	return nil
}

func (dt *DeviceTracker) flushParquetBatchUnsafe() error {
	if len(dt.parquetBatch) == 0 {
		return nil
	}

	if dt.parquetWriter == nil {
		return fmt.Errorf("parquet writer not initialized")
	}

	pool := memory.NewGoAllocator()

	deviceIDBuilder := array.NewStringBuilder(pool)
	eventTimestampBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	latitudeBuilder := array.NewFloat64Builder(pool)
	longitudeBuilder := array.NewFloat64Builder(pool)
	loadDateBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})

	for i := range dt.parquetBatch {
		deviceIDBuilder.Append(dt.parquetBatch[i].DeviceID)
		eventTimestampBuilder.Append(arrow.Timestamp(dt.parquetBatch[i].EventTimestamp.UnixMicro()))
		latitudeBuilder.Append(dt.parquetBatch[i].Latitude)
		longitudeBuilder.Append(dt.parquetBatch[i].Longitude)
		loadDateBuilder.Append(arrow.Timestamp(dt.parquetBatch[i].LoadDate.UnixMicro()))
	}

	deviceIDArray := deviceIDBuilder.NewArray()
	eventTimestampArray := eventTimestampBuilder.NewArray()
	latitudeArray := latitudeBuilder.NewArray()
	longitudeArray := longitudeBuilder.NewArray()
	loadDateArray := loadDateBuilder.NewArray()

	defer deviceIDArray.Release()
	defer eventTimestampArray.Release()
	defer latitudeArray.Release()
	defer longitudeArray.Release()
	defer loadDateArray.Release()

	record := array.NewRecord(
		dt.parquetSchema,
		[]arrow.Array{deviceIDArray, eventTimestampArray, latitudeArray, longitudeArray, loadDateArray},
		int64(len(dt.parquetBatch)),
	)
	defer record.Release()

	if err := dt.parquetWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write parquet batch: %w", err)
	}

	dt.parquetBatch = dt.parquetBatch[:0]
	return nil
}

func (dt *DeviceTracker) closeParquetWriter() {
	dt.parquetMutex.Lock()
	defer dt.parquetMutex.Unlock()
	dt.closeParquetWriterUnsafe()
}

func (dt *DeviceTracker) closeParquetWriterUnsafe() {
	if dt.parquetWriter != nil {
		dt.parquetWriter.Close()
		dt.parquetWriter = nil
	}
	if dt.parquetFile != nil {
		dt.parquetFile.Close()
		dt.parquetFile = nil
	}
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

func (dt *DeviceTracker) extractDateFromPath(path string) string {
	parts := strings.Split(path, string(filepath.Separator))
	for _, part := range parts {
		if strings.HasPrefix(part, "load_date=") {
			return strings.TrimPrefix(part, "load_date=")
		}
	}
	return ""
}

// ============================================================================
// STEP 3: IDLE DEVICE DETECTION
// ============================================================================

func (dt *DeviceTracker) RunIdleDeviceSearch(folderList []string, targetDates []string) error {
	fmt.Println("\n╔════════════════════════════════════════════════════════╗")
	fmt.Println("║   STEP 3: IDLE DEVICE DETECTION                       ║")
	fmt.Println("╚════════════════════════════════════════════════════════╝")

	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for _, targetDate := range targetDates {
		fmt.Printf("\n🔍 Processing Date: %s\n", targetDate)
		err := dt.FindIdleDevicesStreamingByDate(folderList, targetDate)
		if err != nil {
			fmt.Printf("  ⚠️  Error: %v\n", err)
			continue
		}
		runtime.GC()
	}

	err := dt.MergeIdleDevicesByEventDate()
	if err != nil {
		return err
	}

	return nil
}

func (dt *DeviceTracker) FindIdleDevicesStreamingByDate(folderList []string, targetDate string) error {
	campaignMetadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return err
	}
	fmt.Printf("  📋 Loaded %d campaign devices\n", len(campaignMetadata))

	campaignDeviceSet := make(map[string]bool, len(campaignMetadata))
	for deviceID := range campaignMetadata {
		campaignDeviceSet[deviceID] = true
	}

	totalIdle := 0

	for _, folderName := range folderList {
		parquetPath := filepath.Join(
			dt.OutputFolder,
			"time_filtered",
			strings.TrimPrefix(folderName, "load_date="),
			fmt.Sprintf("time_filtered_loaddate%s.parquet", strings.TrimPrefix(folderName, "load_date=")),
		)

		if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
			continue
		}

		fmt.Printf("    📂 Processing: %s\n", filepath.Base(parquetPath))

		idleCount, _, _, err := dt.processParquetFileStreaming(parquetPath, campaignMetadata, campaignDeviceSet)
		if err != nil {
			fmt.Printf("    ⚠️  Error: %v\n", err)
			continue
		}

		totalIdle += idleCount
		runtime.GC()
	}

	fmt.Printf("  ✅ Found %d idle devices\n", totalIdle)
	return nil
}

func (dt *DeviceTracker) processParquetFileStreaming(
	parquetPath string,
	campaignMetadata map[string]CampaignMetadata,
	campaignDeviceSet map[string]bool,
) (int, int, int, error) {

	pf, err := file.OpenParquetFile(parquetPath, false)
	if err != nil {
		return 0, 0, 0, err
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()
	deviceStates := make(map[string]*DeviceTrackingState)
	totalRecords := 0
	filteredRecords := 0

	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		if rgIdx%50 == 0 {
			fmt.Printf("      🔄 Row group %d/%d (tracking %d devices)...\n",
				rgIdx+1, numRowGroups, len(deviceStates))
		}

		records, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}

		totalRecords += len(records)

		filteredRecords += dt.updateDeviceStatesFiltered(
			deviceStates,
			records,
			campaignMetadata,
			campaignDeviceSet,
		)

		records = nil

		if rgIdx%50 == 0 {
			runtime.GC()
		}
	}

	fmt.Printf("      ✅ Analyzed %d devices\n", len(deviceStates))

	idleCount := dt.saveIdleDevicesFromStates(deviceStates, campaignMetadata)

	deviceStates = nil
	runtime.GC()

	return idleCount, totalRecords, filteredRecords, nil
}

func (dt *DeviceTracker) updateDeviceStatesFiltered(
	deviceStates map[string]*DeviceTrackingState,
	records []DeviceRecord,
	campaignMetadata map[string]CampaignMetadata,
	campaignDeviceSet map[string]bool,
) int {
	bufferMeters := dt.IdleDeviceBuffer
	filteredCount := 0

	for i := range records {
		record := &records[i]
		deviceID := record.DeviceID

		if !campaignDeviceSet[deviceID] {
			continue
		}

		filteredCount++

		state, exists := deviceStates[deviceID]
		if !exists {
			deviceStates[deviceID] = &DeviceTrackingState{
				FirstPoint:  orb.Point{record.Longitude, record.Latitude},
				FirstTime:   record.EventTimestamp,
				IsStillIdle: true,
				RecordCount: 1,
			}
			continue
		}

		if state.IsStillIdle {
			point := orb.Point{record.Longitude, record.Latitude}
			distance := geo.Distance(state.FirstPoint, point)

			if distance > bufferMeters {
				state.IsStillIdle = false
			}
		}
		state.RecordCount++
	}

	return filteredCount
}

func (dt *DeviceTracker) saveIdleDevicesFromStates(
	deviceStates map[string]*DeviceTrackingState,
	campaignMetadata map[string]CampaignMetadata,
) int {

	idleDevicesByEventDate := make(map[string][]IdleDeviceResult)

	for deviceID, state := range deviceStates {
		if state.IsStillIdle && state.RecordCount >= 2 {
			metadata, exists := campaignMetadata[deviceID]
			if !exists {
				continue
			}

			idleDevice := IdleDeviceResult{
				DeviceID:    deviceID,
				VisitedTime: metadata.EventTimestamp.Format(time.RFC3339),
				Address:     metadata.Address,
				Campaign:    metadata.Campaign,
				CampaignID:  metadata.CampaignID,
				POIID:       metadata.POIID,
				Geometry:    fmt.Sprintf("POINT (%f %f)", state.FirstPoint[0], state.FirstPoint[1]),
			}

			eventDate := idleDevice.VisitedTime[:10]
			idleDevicesByEventDate[eventDate] = append(idleDevicesByEventDate[eventDate], idleDevice)
		}
	}

	savedCount := 0
	if len(idleDevicesByEventDate) > 0 {
		dt.saveIdleDevicesToFiles(idleDevicesByEventDate)
		for _, devices := range idleDevicesByEventDate {
			savedCount += len(devices)
		}
		fmt.Printf("      💾 Saved %d idle devices\n", savedCount)
	}

	return savedCount
}

func (dt *DeviceTracker) saveIdleDevicesToFiles(idleDevicesByEventDate map[string][]IdleDeviceResult) {
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for eventDate, newDevices := range idleDevicesByEventDate {
		jsonPath := filepath.Join(idleDevicesFolder, fmt.Sprintf("idle_devices_%s.json", eventDate))

		existingDevices := make([]IdleDeviceResult, 0)
		if _, err := os.Stat(jsonPath); err == nil {
			existingData, err := dt.loadIdleDevicesFromJSON(jsonPath)
			if err == nil {
				existingDevices = existingData
			}
		}

		allDevices := append(existingDevices, newDevices...)
		uniqueDevices := dt.deduplicateIdleDevices(allDevices)

		output := IdleDevicesByDate{
			EventDate:        eventDate,
			TotalIdleDevices: len(uniqueDevices),
			IdleDevices:      uniqueDevices,
		}

		file, err := os.Create(jsonPath)
		if err != nil {
			continue
		}

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		encoder.Encode(output)
		file.Close()
	}
}

func (dt *DeviceTracker) loadIdleDevicesFromJSON(jsonPath string) ([]IdleDeviceResult, error) {
	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data IdleDevicesByDate
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}

	return data.IdleDevices, nil
}

func (dt *DeviceTracker) deduplicateIdleDevices(devices []IdleDeviceResult) []IdleDeviceResult {
	seen := make(map[string]bool)
	unique := make([]IdleDeviceResult, 0, len(devices))

	for _, device := range devices {
		key := device.DeviceID
		if !seen[key] {
			seen[key] = true
			unique = append(unique, device)
		}
	}

	return unique
}

func (dt *DeviceTracker) MergeIdleDevicesByEventDate() error {
	fmt.Println("\n📦 Merging idle device files...")

	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	pattern := filepath.Join(idleDevicesFolder, "idle_devices_*.json")

	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Println("  ⚠️  No idle device files found")
		return nil
	}

	allIdleDevicesByDate := make(map[string][]IdleDeviceResult)
	processedDates := make([]string, 0)
	totalDevices := 0

	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		eventDate := strings.TrimPrefix(fileName, "idle_devices_")
		eventDate = strings.TrimSuffix(eventDate, ".json")

		devices, err := dt.loadIdleDevicesFromJSON(filePath)
		if err != nil {
			continue
		}

		allIdleDevicesByDate[eventDate] = devices
		processedDates = append(processedDates, eventDate)
		totalDevices += len(devices)

		fmt.Printf("  ✓ Loaded %d devices from %s\n", len(devices), fileName)
	}

	sort.Strings(processedDates)

	finalOutput := FinalIdleDevicesOutput{
		ProcessedDates:    processedDates,
		TotalIdleDevices:  totalDevices,
		ProcessingTimeMs:  0,
		IdleDevicesByDate: allIdleDevicesByDate,
	}

	finalJSONPath := filepath.Join(dt.OutputFolder, "Idle_devices.json")
	file, err := os.Create(finalJSONPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.Encode(finalOutput)

	fmt.Printf("  💾 Final output: %s\n", finalJSONPath)
	fmt.Printf("  📊 Total: %d idle devices across %d dates\n", totalDevices, len(processedDates))

	return nil
}

func (dt *DeviceTracker) GetUniqIdDataFrame() (map[string]CampaignMetadata, error) {
	yesterday := time.Now().AddDate(0, 0, -1)
	dateStr := yesterday.Format("20060102")

	jsonPath := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr,
		fmt.Sprintf("campaign_devices_%s.json", dateStr))

	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("campaign JSON not found: %s", jsonPath)
	}

	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var campaignOutput CampaignIntersectionOutput
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&campaignOutput); err != nil {
		return nil, err
	}

	metadata := make(map[string]CampaignMetadata, 100000)

	for _, campaign := range campaignOutput.Campaigns {
		for _, poi := range campaign.POIs {
			for _, device := range poi.Devices {
				deviceID := device.DeviceID

				if _, exists := metadata[deviceID]; exists {
					continue
				}

				metadata[deviceID] = CampaignMetadata{
					DeviceID:       deviceID,
					EventTimestamp: device.EventTimestamp,
					Address:        poi.POIName,
					Campaign:       campaign.CampaignName,
					CampaignID:     campaign.CampaignID,
					POIID:          poi.POIID,
				}
			}
		}
	}

	return metadata, nil
}

func (dt *DeviceTracker) GetUniqueIdList() ([]string, error) {
	metadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return nil, err
	}

	idList := make([]string, 0, len(metadata))
	for deviceID := range metadata {
		idList = append(idList, deviceID)
	}

	return idList, nil
}

// ============================================================================
// MAIN
// ============================================================================

func GetLastNDatesFromYesterday(n int) []string {
	dates := make([]string, n)

	for i := 0; i < n; i++ {
		date := time.Now().AddDate(0, 0, -(i + 1))
		dates[i] = date.Format("2006-01-02")
	}

	return dates
}

func RunDeviceTracker(runSteps []int) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	yesterday := time.Now().AddDate(0, 0, -1)
	yesterdayStr := yesterday.Format("2006-01-02")

	dates := []string{yesterdayStr}

	folderList := make([]string, 0, len(dates))
	for _, d := range dates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		folderList = append(folderList, folder)
	}

	fmt.Printf("Folder List: %v\n", folderList)
	fmt.Printf("CPU Cores: %d\n", runtime.NumCPU())

	mongoConfig := MongoConfig{
		URI:        "mongodb://admin:nyros%4006@localhost:27017",
		Database:   "locatrix",
		Collection: "devices_within_campaign",
	}

	dt, err := NewDeviceTracker(
		"https://locatrix-backend-development.up.railway.app/api/admin/activecampaign/list",
		"/home/device-tracker/data/output",
		mongoConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to create device tracker: %w", err)
	}

	defer dt.Close()

	step1 := containsStep(runSteps, 1)
	step2 := containsStep(runSteps, 2)
	step3 := containsStep(runSteps, 3)

	if step1 || step2 || step3 {
		if err := dt.fetchCampaignsFromAPI(); err != nil {
			return fmt.Errorf("failed to fetch campaigns from API: %w", err)
		}

		dt.buildSpatialIndex()
	}

	if step1 || step2 {
		runningSteps := ""

		if step1 && step2 {
			runningSteps = "Campaign Intersection & Time Filtering"
		} else if step1 {
			runningSteps = "Campaign Intersection"
		} else if step2 {
			runningSteps = "Time Filtering"
		}

		fmt.Printf("\n========== Running %s ==========\n", runningSteps)
		for _, folder := range folderList {
			err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/"+folder, step1, step2)
			if err != nil {
				fmt.Printf("Error in %s for %s: %v\n", runningSteps, folder, err)
			}
		}

		fmt.Printf("\n%s Completed\n", runningSteps)
	}

	if step3 {
		err := dt.RunIdleDeviceSearch(folderList, dates)
		if err != nil {
			log.Fatal(err)
		}
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
	fmt.Println("   Device Tracker - Complete Fixed")
	fmt.Println("===========================================")

	startTime := time.Now()

	// IMPORTANT: Run steps in order!
	// First time: runSteps := []int{1, 2}  // Creates campaign + time-filtered data
	// Second time: runSteps := []int{3}    // Finds idle devices
	runSteps := []int{1, 2, 3} // Change to []int{3} after Step 1 & 2 complete

	err := RunDeviceTracker(runSteps)
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
