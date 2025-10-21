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
	"strconv"
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
	"github.com/paulmach/orb/planar"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	workerPoolSize   = 20
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

// MongoDB document structure - each POI as separate document
type POIMongoDocument struct {
	ID            primitive.ObjectID    `bson:"_id,omitempty"`
	POIID         primitive.ObjectID    `bson:"poi_id"`
	POIName       string                `bson:"poi_name"`
	ProcessedDate string                `bson:"processed_date"`
	DeviceCount   int                   `bson:"device_count"`
	Devices       []MinimalDeviceRecord `bson:"devices"`
	CreatedAt     time.Time             `bson:"created_at"`
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

func (dt *DeviceTracker) initParquetWriter(dateStr string) error {
	dt.parquetMutex.Lock()
	defer dt.parquetMutex.Unlock()

	if dt.parquetWriter != nil && dt.currentDate == dateStr {
		return nil
	}

	if dt.parquetWriter != nil {
		dt.closeParquetWriterUnsafe()
	}

	step5Folder := filepath.Join(dt.OutputFolder, "step5_time_filtered", dateStr)
	os.MkdirAll(step5Folder, 0755)

	// Format: time_filtered_loaddate20251020.parquet
	parquetPath := filepath.Join(step5Folder, fmt.Sprintf("time_filtered_loaddate%s.parquet", dateStr))

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

	fmt.Printf("✅ Initialized Parquet writer for date: %s at %s\n", dateStr, parquetPath)

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
		return fmt.Errorf("failed to write to parquet: %w", err)
	}

	fmt.Printf("Flushed %d records to Parquet\n", len(dt.parquetBatch))

	dt.parquetBatch = dt.parquetBatch[:0]

	return nil
}

func (dt *DeviceTracker) closeParquetWriterUnsafe() error {
	if dt.parquetWriter == nil {
		return nil
	}

	if len(dt.parquetBatch) > 0 {
		if err := dt.flushParquetBatchUnsafe(); err != nil {
			fmt.Printf("Error flushing final batch: %v\n", err)
		}
	}

	if err := dt.parquetWriter.Close(); err != nil {
		fmt.Printf("Error closing parquet writer: %v\n", err)
	}

	if dt.parquetFile != nil {
		dt.parquetFile.Close()
	}

	dt.parquetWriter = nil
	dt.parquetFile = nil

	return nil
}

func (dt *DeviceTracker) saveToMongoDB(campaignMap map[string]map[string][]MinimalDeviceRecord,
	campaignNames map[string]string,
	poiNames map[string]map[string]string,
	processedDate string) error {

	if dt.mongoCollection == nil {
		fmt.Println("MongoDB not configured, skipping MongoDB save")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var documents []interface{}
	createdAt := time.Now()

	for campaignID, poisMap := range campaignMap {
		for poiID, devices := range poisMap {
			poiObjectID, err := primitive.ObjectIDFromHex(poiID)
			if err != nil {
				fmt.Printf("Warning: Invalid POI ID %s, skipping: %v\n", poiID, err)
				continue
			}

			doc := POIMongoDocument{
				POIID:         poiObjectID,
				POIName:       poiNames[campaignID][poiID],
				ProcessedDate: processedDate,
				DeviceCount:   len(devices),
				Devices:       devices,
				CreatedAt:     createdAt,
			}
			documents = append(documents, doc)
		}
	}

	if len(documents) == 0 {
		fmt.Println("No documents to insert into MongoDB")
		return nil
	}

	result, err := dt.mongoCollection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert documents into MongoDB: %w", err)
	}

	fmt.Printf("✅ Successfully inserted %d POI documents into MongoDB\n", len(result.InsertedIDs))
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

				if err := dt.addToParquetBatch(tfRecord, dateStr); err != nil {
					fmt.Printf("Error adding to Parquet batch: %v\n", err)
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

func NewDeviceTracker(campaignAPIURL, outputFolder string, mongoConfig MongoConfig) (*DeviceTracker, error) {
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	var mongoClient *mongo.Client
	var mongoCollection *mongo.Collection

	if mongoConfig.URI != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		clientOptions := options.Client().ApplyURI(mongoConfig.URI)
		var err error
		mongoClient, err = mongo.Connect(ctx, clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
		}

		if err := mongoClient.Ping(ctx, nil); err != nil {
			return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
		}

		fmt.Println("✅ Successfully connected to MongoDB")

		mongoCollection = mongoClient.Database(mongoConfig.Database).Collection(mongoConfig.Collection)
	}

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
		parquetBatch:     make([]TimeFilteredRecord, 0, parquetBatchSize),
		mongoClient:      mongoClient,
		mongoCollection:  mongoCollection,
		mongoConfig:      mongoConfig,
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
	dt.parquetMutex.Lock()
	defer dt.parquetMutex.Unlock()

	if err := dt.closeParquetWriterUnsafe(); err != nil {
		fmt.Printf("Error closing parquet writer: %v\n", err)
	}

	if dt.mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dt.mongoClient.Disconnect(ctx)
	}

	return nil
}

func (dt *DeviceTracker) addToParquetBatch(record TimeFilteredRecord, dateStr string) error {
	dt.parquetMutex.Lock()
	defer dt.parquetMutex.Unlock()

	if dt.parquetWriter == nil || dt.currentDate != dateStr {
		dt.parquetMutex.Unlock()
		if err := dt.initParquetWriter(dateStr); err != nil {
			dt.parquetMutex.Lock()
			return err
		}
		dt.parquetMutex.Lock()
	}

	dt.parquetBatch = append(dt.parquetBatch, record)

	if len(dt.parquetBatch) >= parquetBatchSize {
		return dt.flushParquetBatchUnsafe()
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

	dateStr := dt.extractDateFromPath(parquetFolder)

	if step5 {
		if err := dt.initParquetWriter(dateStr); err != nil {
			return fmt.Errorf("failed to initialize parquet writer: %w", err)
		}
	}

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

	seen := make(map[string]struct{})
	unique := make([]DeviceRecord, 0, len(allRecords))
	for i := range allRecords {
		if _, exists := seen[allRecords[i].DeviceID]; !exists {
			seen[allRecords[i].DeviceID] = struct{}{}
			unique = append(unique, allRecords[i])
		}
	}

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
			EventTimestamp: unique[i].EventTimestamp,
		}

		campaignMap[campaignID][poiID] = append(campaignMap[campaignID][poiID], minimalDevice)
		campaignNames[campaignID] = unique[i].Campaign
		poiNames[campaignID][poiID] = unique[i].Address
	}

	if dt.mongoCollection != nil {
		fmt.Println("\n💾 Saving to MongoDB...")
		if err := dt.saveToMongoDB(campaignMap, campaignNames, poiNames, dateStr); err != nil {
			fmt.Printf("Warning: MongoDB save failed: %v\n", err)
		}
	}

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

		fmt.Printf("✅ Time-filtered data saved to Parquet in: %s\n", step5Folder)
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
		"2025-10-20",
	}

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
	fmt.Println("   Device Tracker - Parquet Output")
	fmt.Println("===========================================")

	startTime := time.Now()

	runSteps := []int{3, 5}

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
