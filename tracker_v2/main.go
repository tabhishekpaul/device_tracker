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
	workerPoolSize   = 48
	rowGroupWorkers  = 48
	fileWorkers      = 16
	step3Workers     = 48
)

type MongoConfig struct {
	URI        string
	Database   string
	Collection string
}

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

	parquetWriters     []*ParquetWriter
	parquetWriterMutex sync.Mutex
	numWriters         int

	mongoClient     *mongo.Client
	mongoCollection *mongo.Collection
	mongoConfig     MongoConfig
}

type ParquetWriter struct {
	mutex  sync.Mutex
	batch  []TimeFilteredRecord
	writer *pqarrow.FileWriter
	file   *os.File
	schema *arrow.Schema
	date   string
	id     int
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

type CampaignMetadata struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Address        string    `json:"address"`
	Campaign       string    `json:"campaign"`
	CampaignID     string    `json:"campaign_id"`
	POIID          string    `json:"poi_id"`
	LoadDate       string    `json:"load_date"`
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

type FileJob struct {
	FilePath string
	Index    int
}

type RowGroupJob struct {
	FilePath    string
	RowGroupIdx int
}

type RowGroupResult struct {
	Records []DeviceRecord
	Error   error
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
		NumWorkers:       workerPoolSize,
		mongoConfig:      mongoConfig,
		numWriters:       8,
	}

	dt.parseTimeFilters()

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

	fmt.Printf("⚙️  Cores: %d | Workers: %d | RAM: 386GB\n", runtime.NumCPU(), workerPoolSize)
	fmt.Printf("⚙️  Time filter: %s to %s\n", dt.FilterInTime, dt.FilterOutTime)
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

	fmt.Println("✅ MongoDB connected")
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
	resp, err := http.Get(dt.CampaignAPIURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return err
	}

	if !apiResp.Success {
		return fmt.Errorf("API error: %s", apiResp.Message)
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

	fmt.Printf("✅ Loaded %d POIs from %d campaigns\n", len(dt.LocData), len(apiResp.Data))
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

	fmt.Printf("✅ Spatial index: %d cells\n", len(dt.spatialIndex))
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
// STEP 1: HYPER-PARALLEL CAMPAIGN INTERSECTION
// ============================================================================

func (dt *DeviceTracker) FindCampaignIntersectionForFolder(folderPath string, runStep1, runStep2 bool) error {
	dateStr := dt.extractDateFromPath(folderPath)
	if dateStr == "" {
		return fmt.Errorf("could not extract date from path: %s", folderPath)
	}

	if runStep1 {
		fmt.Printf("\n🚀 STEP 1: Campaign Intersection [%s] - %d WORKERS\n", dateStr, fileWorkers)

		parquetFiles, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
		if err != nil {
			return err
		}

		fmt.Printf("  📂 Processing %d files in parallel...\n", len(parquetFiles))

		deviceMap := dt.processCampaignIntersectionParallel(parquetFiles)

		err = dt.saveCampaignIntersectionJSON(deviceMap, dateStr)
		if err != nil {
			return err
		}

		fmt.Printf("  ✅ Step 1: %d unique devices\n", len(deviceMap))
	}

	if runStep2 {
		fmt.Printf("\n🚀 STEP 2: Time Filtering [%s] - %d WORKERS\n", dateStr, fileWorkers)

		campaignDeviceSet, err := dt.loadCampaignDeviceSetFromJSON(dateStr)
		if err != nil {
			return err
		}

		fmt.Printf("  📋 Filtering %d campaign devices\n", len(campaignDeviceSet))

		dt.NTFDC.Store(0)

		err = dt.initParquetWriters(dateStr)
		if err != nil {
			return err
		}

		parquetFiles, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
		if err != nil {
			dt.closeAllParquetWriters()
			return err
		}

		fmt.Printf("  📂 Processing %d files with time+campaign filter...\n", len(parquetFiles))

		dt.processTimeFilteringParallel(parquetFiles, campaignDeviceSet)

		dt.closeAllParquetWriters()

		fmt.Printf("  ✅ Step 2: %d records filtered\n", dt.NTFDC.Load())
	}

	return nil
}

// ============================================================================
// HYPER-PARALLEL CAMPAIGN INTERSECTION
// ============================================================================

func (dt *DeviceTracker) processCampaignIntersectionParallel(parquetFiles []string) map[string]map[string]MinimalDeviceRecord {
	deviceMap := make(map[string]map[string]MinimalDeviceRecord)
	var mu sync.Mutex

	jobs := make(chan FileJob, len(parquetFiles))
	var wg sync.WaitGroup

	for w := 0; w < fileWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range jobs {
				localMap := dt.processFileForCampaignIntersection(job.FilePath)

				if len(localMap) > 0 {
					mu.Lock()
					for key, data := range localMap {
						if _, exists := deviceMap[key]; !exists {
							deviceMap[key] = data
						}
					}
					mu.Unlock()
				}

				if job.Index%50 == 0 {
					fmt.Printf("    ⚡ Worker %d: File %d/%d processed\n", workerID, job.Index+1, len(parquetFiles))
				}
			}
		}(w)
	}

	for i, fpath := range parquetFiles {
		jobs <- FileJob{FilePath: fpath, Index: i}
	}
	close(jobs)

	wg.Wait()

	return deviceMap
}

func (dt *DeviceTracker) processFileForCampaignIntersection(parquetFile string) map[string]map[string]MinimalDeviceRecord {
	localMap := make(map[string]map[string]MinimalDeviceRecord)

	pf, err := file.OpenParquetFile(parquetFile, false)
	if err != nil {
		return localMap
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()

	rgJobs := make(chan int, numRowGroups)
	rgResults := make(chan []DeviceRecord, numRowGroups)
	var rgWg sync.WaitGroup

	for w := 0; w < rowGroupWorkers; w++ {
		rgWg.Add(1)
		go func() {
			defer rgWg.Done()
			for rgIdx := range rgJobs {
				records, err := dt.readRowGroup(pf, rgIdx)
				if err == nil && len(records) > 0 {
					rgResults <- records
				}
			}
		}()
	}

	go func() {
		for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
			rgJobs <- rgIdx
		}
		close(rgJobs)
	}()

	go func() {
		rgWg.Wait()
		close(rgResults)
	}()

	for records := range rgResults {
		for _, record := range records {
			point := orb.Point{record.Longitude, record.Latitude}
			locIndices := dt.findIntersectingLocations(point)

			if len(locIndices) > 0 {
				for _, locIdx := range locIndices {
					loc := dt.LocData[locIdx]
					key := fmt.Sprintf("%s_%s_%s", loc.CampaignID, loc.POIID, record.DeviceID)

					if _, exists := localMap[key]; !exists {
						localMap[key] = map[string]MinimalDeviceRecord{
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
			}
		}
	}

	return localMap
}

// ============================================================================
// HYPER-PARALLEL TIME FILTERING
// ============================================================================

func (dt *DeviceTracker) processTimeFilteringParallel(parquetFiles []string, campaignDeviceSet map[string]bool) {
	jobs := make(chan FileJob, len(parquetFiles))
	var wg sync.WaitGroup

	for w := 0; w < fileWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range jobs {
				dt.processFileForTimeFiltering(job.FilePath, campaignDeviceSet, workerID)

				if job.Index%10 == 0 {
					fmt.Printf("    ⚡ Worker %d: File %d/%d | Total: %d\n",
						workerID, job.Index+1, len(parquetFiles), dt.NTFDC.Load())
				}
			}
		}(w)
	}

	for i, fpath := range parquetFiles {
		jobs <- FileJob{FilePath: fpath, Index: i}
	}
	close(jobs)

	wg.Wait()
}

func (dt *DeviceTracker) processFileForTimeFiltering(parquetFile string, campaignDeviceSet map[string]bool, workerID int) {
	pf, err := file.OpenParquetFile(parquetFile, false)
	if err != nil {
		return
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()

	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		records, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}

		for i := range records {
			record := &records[i]

			// CRITICAL: Verify device_id is not empty
			if record.DeviceID == "" {
				continue
			}

			// Filter by campaign
			if !campaignDeviceSet[record.DeviceID] {
				continue
			}

			// Filter by time
			if dt.isWithinTimeFilter(record.EventTimestamp) {
				writerIdx := workerID % dt.numWriters
				dt.addToParquetWriter(writerIdx, TimeFilteredRecord{
					DeviceID:       record.DeviceID,
					EventTimestamp: record.EventTimestamp,
					Latitude:       record.Latitude,
					Longitude:      record.Longitude,
					LoadDate:       time.Now(),
				})

				dt.NTFDC.Add(1)
			}
		}
	}
}

// ============================================================================
// MULTIPLE PARQUET WRITERS - FIXED VERSION
// ============================================================================

func (dt *DeviceTracker) initParquetWriters(dateStr string) error {
	dt.parquetWriterMutex.Lock()
	defer dt.parquetWriterMutex.Unlock()

	timeFilteredFolder := filepath.Join(dt.OutputFolder, "time_filtered", dateStr)
	os.MkdirAll(timeFilteredFolder, 0755)

	dt.parquetWriters = make([]*ParquetWriter, dt.numWriters)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "device_id", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "event_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
			{Name: "latitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "longitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "load_date", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		},
		nil,
	)

	for i := 0; i < dt.numWriters; i++ {
		parquetPath := filepath.Join(timeFilteredFolder, fmt.Sprintf("time_filtered_loaddate%s_part%d.parquet", dateStr, i))

		file, err := os.Create(parquetPath)
		if err != nil {
			return err
		}

		props := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
			parquet.WithDictionaryDefault(true),
		)

		writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
		if err != nil {
			file.Close()
			return err
		}

		dt.parquetWriters[i] = &ParquetWriter{
			batch:  make([]TimeFilteredRecord, 0, parquetBatchSize),
			writer: writer,
			file:   file,
			schema: schema,
			date:   dateStr,
			id:     i,
		}
	}

	fmt.Printf("  ✅ Initialized %d parallel parquet writers\n", dt.numWriters)
	return nil
}

func (dt *DeviceTracker) addToParquetWriter(writerIdx int, record TimeFilteredRecord) {
	// CRITICAL VALIDATION: Check device_id is not empty
	if record.DeviceID == "" {
		fmt.Printf("⚠️  WARNING: Attempting to write empty device_id\n")
		return
	}

	pw := dt.parquetWriters[writerIdx]
	pw.mutex.Lock()
	defer pw.mutex.Unlock()

	pw.batch = append(pw.batch, record)

	if len(pw.batch) >= parquetBatchSize {
		dt.flushParquetWriter(pw)
	}
}

// CRITICAL FIX: Properly write string columns with validation
func (dt *DeviceTracker) flushParquetWriter(pw *ParquetWriter) error {
	if len(pw.batch) == 0 {
		return nil
	}

	pool := memory.NewGoAllocator()

	deviceIDBuilder := array.NewStringBuilder(pool)
	eventTimestampBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	latitudeBuilder := array.NewFloat64Builder(pool)
	longitudeBuilder := array.NewFloat64Builder(pool)
	loadDateBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})

	// CRITICAL: Validate and append each value
	validCount := 0
	for i := range pw.batch {
		// Skip records with empty device_id
		if pw.batch[i].DeviceID == "" {
			fmt.Printf("⚠️  Skipping record with empty device_id\n")
			continue
		}

		deviceIDBuilder.Append(pw.batch[i].DeviceID)
		eventTimestampBuilder.Append(arrow.Timestamp(pw.batch[i].EventTimestamp.UnixMicro()))
		latitudeBuilder.Append(pw.batch[i].Latitude)
		longitudeBuilder.Append(pw.batch[i].Longitude)
		loadDateBuilder.Append(arrow.Timestamp(pw.batch[i].LoadDate.UnixMicro()))
		validCount++
	}

	if validCount == 0 {
		fmt.Printf("⚠️  Writer %d: All records had empty device_id, skipping flush\n", pw.id)
		pw.batch = pw.batch[:0]
		return nil
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
		pw.schema,
		[]arrow.Array{deviceIDArray, eventTimestampArray, latitudeArray, longitudeArray, loadDateArray},
		int64(validCount),
	)
	defer record.Release()

	if err := pw.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write parquet batch: %w", err)
	}

	pw.batch = pw.batch[:0]
	return nil
}

func (dt *DeviceTracker) closeAllParquetWriters() {
	dt.parquetWriterMutex.Lock()
	defer dt.parquetWriterMutex.Unlock()

	for _, pw := range dt.parquetWriters {
		if pw != nil {
			pw.mutex.Lock()
			dt.flushParquetWriter(pw)
			pw.writer.Close()
			pw.file.Close()
			pw.mutex.Unlock()
		}
	}
}

// ============================================================================
// STEP 3: HYPER-PARALLEL IDLE DEVICE DETECTION
// ============================================================================

func (dt *DeviceTracker) RunIdleDeviceSearch(folderList []string, targetDates []string) error {
	fmt.Println("\n🚀 STEP 3: IDLE DEVICE DETECTION - FULL PARALLEL")

	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	fmt.Println("\n  📥 Loading campaign metadata from all dates...")
	allCampaignMetadata, err := dt.GetUniqIdDataFrameForAllDates(targetDates)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	campaignDeviceSet := make(map[string]bool, len(allCampaignMetadata))
	for deviceID := range allCampaignMetadata {
		campaignDeviceSet[deviceID] = true
	}

	for _, targetDate := range targetDates {
		fmt.Printf("\n🔍 Date: %s [%d WORKERS]\n", targetDate, step3Workers)
		err := dt.FindIdleDevicesHyperParallelFixed(folderList, targetDate, allCampaignMetadata, campaignDeviceSet)
		if err != nil {
			fmt.Printf("  ⚠️  Error: %v\n", err)
			continue
		}
		runtime.GC()
	}

	dt.MergeIdleDevicesByEventDate()
	return nil
}

func (dt *DeviceTracker) FindIdleDevicesHyperParallelFixed(
	folderList []string,
	targetDate string,
	campaignMetadata map[string]CampaignMetadata,
	campaignDeviceSet map[string]bool,
) error {

	dateStr := strings.ReplaceAll(targetDate, "-", "")
	timeFilteredFolder := filepath.Join(dt.OutputFolder, "time_filtered", dateStr)
	pattern := filepath.Join(timeFilteredFolder, "*.parquet")

	allParquetFiles, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(allParquetFiles) == 0 {
		fmt.Printf("  ⚠️  No files in %s\n", timeFilteredFolder)
		return nil
	}

	fmt.Printf("  📂 Processing %d files\n", len(allParquetFiles))

	deviceStates := make(map[string]*DeviceTrackingState)
	var statesMutex sync.Mutex

	jobs := make(chan string, len(allParquetFiles))
	var wg sync.WaitGroup

	for w := 0; w < step3Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for parquetPath := range jobs {
				localStates := dt.processParquetForIdleDevices(parquetPath, campaignDeviceSet)

				statesMutex.Lock()
				for deviceID, localState := range localStates {
					globalState, exists := deviceStates[deviceID]
					if !exists {
						deviceStates[deviceID] = localState
					} else {
						if globalState.IsStillIdle && localState.IsStillIdle {
							distance := geo.Distance(globalState.FirstPoint, localState.FirstPoint)
							if distance > dt.IdleDeviceBuffer {
								globalState.IsStillIdle = false
							}
						}
						globalState.RecordCount += localState.RecordCount
					}
				}
				statesMutex.Unlock()
			}
		}(w)
	}

	for _, fpath := range allParquetFiles {
		jobs <- fpath
	}
	close(jobs)

	wg.Wait()

	fmt.Printf("  ✅ Analyzed %d devices\n", len(deviceStates))

	idleCount := dt.saveIdleDevicesFromStates(deviceStates, campaignMetadata)
	fmt.Printf("  💾 Saved %d idle devices\n", idleCount)

	return nil
}

func (dt *DeviceTracker) processParquetForIdleDevices(parquetPath string, campaignDeviceSet map[string]bool) map[string]*DeviceTrackingState {
	localStates := make(map[string]*DeviceTrackingState)

	pf, err := file.OpenParquetFile(parquetPath, false)
	if err != nil {
		return localStates
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()

	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		records, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}

		for i := range records {
			record := &records[i]
			deviceID := record.DeviceID

			// Skip empty device IDs
			if deviceID == "" {
				continue
			}

			if !campaignDeviceSet[deviceID] {
				continue
			}

			state, exists := localStates[deviceID]
			if !exists {
				localStates[deviceID] = &DeviceTrackingState{
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

				if distance > dt.IdleDeviceBuffer {
					state.IsStillIdle = false
				}
			}
			state.RecordCount++
		}
	}

	return localStates
}

// ============================================================================
// REMAINING METHODS
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

func (dt *DeviceTracker) loadCampaignDeviceSetFromJSON(dateStr string) (map[string]bool, error) {
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

func (dt *DeviceTracker) GetUniqIdDataFrameForDate(dateStr string) (map[string]CampaignMetadata, error) {
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
					LoadDate:       dateStr,
				}
			}
		}
	}

	return metadata, nil
}

func (dt *DeviceTracker) GetUniqIdDataFrameForAllDates(dates []string) (map[string]CampaignMetadata, error) {
	allMetadata := make(map[string]CampaignMetadata, 500000)
	loadedDates := 0

	for _, date := range dates {
		dateStr := strings.ReplaceAll(date, "-", "")

		metadata, err := dt.GetUniqIdDataFrameForDate(dateStr)
		if err != nil {
			fmt.Printf("    ⚠️  Skip %s: %v\n", date, err)
			continue
		}

		for deviceID, meta := range metadata {
			if _, exists := allMetadata[deviceID]; !exists {
				allMetadata[deviceID] = meta
			}
		}

		loadedDates++
		fmt.Printf("    ✓ Loaded %d devices from %s\n", len(metadata), date)
	}

	if len(allMetadata) == 0 {
		return nil, fmt.Errorf("no metadata found")
	}

	fmt.Printf("  📋 Total unique campaign devices across %d dates: %d\n", loadedDates, len(allMetadata))
	return allMetadata, nil
}

func (dt *DeviceTracker) GetUniqIdDataFrame() (map[string]CampaignMetadata, error) {
	yesterday := time.Now().AddDate(0, 0, -1)
	dateStr := yesterday.Format("20060102")
	return dt.GetUniqIdDataFrameForDate(dateStr)
}

func (dt *DeviceTracker) saveCampaignIntersectionJSON(deviceMap map[string]map[string]MinimalDeviceRecord, dateStr string) error {
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

func (dt *DeviceTracker) saveIdleDevicesFromStates(
	deviceStates map[string]*DeviceTrackingState,
	campaignMetadata map[string]CampaignMetadata,
) int {

	idleDevicesByLoadDate := make(map[string][]IdleDeviceResult)

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

			loadDate := metadata.LoadDate
			idleDevicesByLoadDate[loadDate] = append(idleDevicesByLoadDate[loadDate], idleDevice)
		}
	}

	savedCount := 0
	if len(idleDevicesByLoadDate) > 0 {
		dt.saveIdleDevicesToFiles(idleDevicesByLoadDate)
		for _, devices := range idleDevicesByLoadDate {
			savedCount += len(devices)
		}
	}

	return savedCount
}

func (dt *DeviceTracker) saveIdleDevicesToFiles(idleDevicesByLoadDate map[string][]IdleDeviceResult) {
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for loadDate, newDevices := range idleDevicesByLoadDate {
		jsonPath := filepath.Join(idleDevicesFolder, fmt.Sprintf("idle_devices_%s.json", loadDate))

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
			EventDate:        loadDate,
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
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	pattern := filepath.Join(idleDevicesFolder, "idle_devices_*.json")

	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
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

	fmt.Printf("\n💾 Final: %d idle devices across %d dates\n", totalDevices, len(processedDates))
	return nil
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
			fmt.Printf("Panic reading string: %v\n", r)
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

		// CRITICAL FIX: Check if column is nullable
		maxDefLevel := reader.Descriptor().MaxDefinitionLevel()

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}

			// For non-nullable (maxDefLevel=0): defLevel=0 means present
			// For nullable (maxDefLevel>0): defLevel>0 means present
			for i := 0; i < int(n); i++ {
				if maxDefLevel == 0 || defLevels[i] > 0 {
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
			fmt.Printf("Panic reading timestamp: %v\n", r)
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

		// CRITICAL FIX: Check if column is nullable
		maxDefLevel := reader.Descriptor().MaxDefinitionLevel()

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if maxDefLevel == 0 || defLevels[i] > 0 {
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

func (dt *DeviceTracker) readFloatColumn(rg *file.RowGroupReader, colIdx int, numRows int) []float64 {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic reading float: %v\n", r)
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

		// CRITICAL FIX: Check if column is nullable
		maxDefLevel := reader.Descriptor().MaxDefinitionLevel()

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if maxDefLevel == 0 || defLevels[i] > 0 {
					result = append(result, values[i])
				} else {
					result = append(result, 0.0)
				}
			}
		}
	case *file.Float32ColumnChunkReader:
		values := make([]float32, 8192)
		defLevels := make([]int16, 8192)

		// CRITICAL FIX: Check if column is nullable
		maxDefLevel := reader.Descriptor().MaxDefinitionLevel()

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}
			for i := 0; i < int(n); i++ {
				if maxDefLevel == 0 || defLevels[i] > 0 {
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

	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	ydates := []string{
		yesterday,
	}

	yfolderList := make([]string, 0, len(ydates))
	for _, d := range ydates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		yfolderList = append(yfolderList, folder)
	}

	fmt.Printf("📅 Dates: %v\n", ydates)

	mongoConfig := MongoConfig{
		URI:        "mongodb://admin:nyros%4006@localhost:27017",
		Database:   "locatrix",
		Collection: "devices_within_campaign",
	}
	outputFolder := "/home/device-tracker/data/output"

	dt, err := NewDeviceTracker(
		"https://locatrix-backend-development.up.railway.app/api/admin/activecampaign/list",
		outputFolder,
		mongoConfig,
	)
	if err != nil {
		return err
	}
	defer dt.Close()

	step1 := containsStep(runSteps, 1)
	step2 := containsStep(runSteps, 2)
	step3 := containsStep(runSteps, 3)
	step4 := containsStep(runSteps, 4)

	if step1 || step2 || step3 {
		if err := dt.fetchCampaignsFromAPI(); err != nil {
			return err
		}
		dt.buildSpatialIndex()
	}

	if step1 || step2 {
		fmt.Printf("\n🚀 HYPER-PARALLEL MODE: %d CORES\n", runtime.NumCPU())
		for _, folder := range yfolderList {
			err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/"+folder, step1, step2)
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			}
		}
	}

	dates := GetLastNDatesFromYesterday(7)

	folderList := make([]string, 0, len(dates))
	for _, d := range dates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		folderList = append(folderList, folder)
	}

	if step3 {
		err := dt.RunIdleDeviceSearch(folderList, dates)
		if err != nil {
			log.Fatal(err)
		}
	}

	if step4 {
		yesterday := "20251021" //ime.Now().AddDate(0, 0, -1).Format("20060102")

		consumerFolder := filepath.Join(outputFolder, "consumers")
		idleDevicesPath := filepath.Join(outputFolder, fmt.Sprintf("idle_devices/idle_devices_%s.json", yesterday))

		matcher := NewConsumerDeviceMatcher(outputFolder, consumerFolder, idleDevicesPath)

		if err := matcher.Run(); err != nil {
			log.Fatalf("Error: %v", err)
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
	fmt.Println("╔═══════════════════════════════════════════════════════╗")
	fmt.Println("║     DEVICE TRACKER - HYPER-PARALLEL EDITION          ║")
	fmt.Println("║     48 CORES | 386GB RAM | 2000%+ CPU TARGET         ║")
	fmt.Println("║     FIXED: Proper Parquet String Writing            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════╝")

	startTime := time.Now()

	// CRITICAL: Delete old time_filtered files and re-run Steps 1 & 2
	// The existing files have NULL device_ids and cannot be fixed
	runSteps := []int{4} // Re-run to create proper files

	err := RunDeviceTracker(runSteps)
	if err != nil {
		fmt.Printf("\n❌ Error: %v\n", err)
		os.Exit(1)
	}

	duration := time.Since(startTime)
	fmt.Printf("\n╔═══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ ✅ COMPLETED IN %v\n", duration)
	fmt.Printf("╚═══════════════════════════════════════════════════════╝\n")
}
