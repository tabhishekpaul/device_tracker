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
	channelBuffer    = 100
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

	// 🆕 Store detected timezone from source files
	sourceTimezone string
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
		sourceTimezone:   "", // Will be detected from source files
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
	fmt.Printf("⚙️  Time filter: %s to %s (LOCAL TIME)\n", dt.FilterInTime, dt.FilterOutTime)
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
// TIMEZONE DETECTION
// ============================================================================

// 🆕 Detect timezone from source parquet file
func (dt *DeviceTracker) detectSourceTimezone(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return "", err
	}

	schema, err := arrowReader.Schema()
	if err != nil {
		return "", err
	}

	// Find timestamp column and get its timezone
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		if field.Name == dt.TimeColumnName {
			if tsType, ok := field.Type.(*arrow.TimestampType); ok {
				if tsType.TimeZone != "" {
					fmt.Printf("✅ Detected timezone from source: %s\n", tsType.TimeZone)
					return tsType.TimeZone, nil
				}
			}
		}
	}

	// No timezone found, use UTC as default
	fmt.Printf("⚠️  No timezone in source, using UTC\n")
	return "UTC", nil
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

	// 🆕 Detect timezone from first file
	if dt.sourceTimezone == "" {
		files, _ := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
		if len(files) > 0 {
			tz, err := dt.detectSourceTimezone(files[0])
			if err == nil {
				dt.sourceTimezone = tz
			} else {
				dt.sourceTimezone = "UTC"
			}
		}
	}

	if runStep1 {
		fmt.Printf("\n🚀 STEP 1: Campaign Intersection [%s] - %d WORKERS\n", dateStr, fileWorkers)
		start := time.Now()

		if err := dt.processCampaignIntersection(folderPath, dateStr); err != nil {
			return err
		}

		elapsed := time.Since(start)
		fmt.Printf("✅ Step 1 completed in %.2fs\n", elapsed.Seconds())
	}

	if runStep2 {
		fmt.Printf("\n🚀 STEP 2: Time Filtering [%s] - %d WRITERS\n", dateStr, dt.numWriters)
		fmt.Printf("📍 Using timezone: %s for LOCAL time filtering\n", dt.sourceTimezone)
		start := time.Now()

		if err := dt.initParquetWriters(dateStr); err != nil {
			return err
		}

		if err := dt.processTimeFiltering(folderPath, dateStr); err != nil {
			return err
		}

		dt.closeParquetWriters()

		// Merge parquet files by event date
		if err := dt.mergeParquetFilesByEventDate(dateStr); err != nil {
			return fmt.Errorf("failed to merge parquet files: %w", err)
		}

		elapsed := time.Since(start)
		fmt.Printf("✅ Step 2 completed in %.2fs\n", elapsed.Seconds())
	}

	return nil
}

func (dt *DeviceTracker) processCampaignIntersection(folderPath, dateStr string) error {
	files, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no parquet files found in %s", folderPath)
	}

	fmt.Printf("📂 Found %d parquet files\n", len(files))

	fileJobs := make(chan FileJob, len(files))
	for i, f := range files {
		fileJobs <- FileJob{FilePath: f, Index: i}
	}
	close(fileJobs)

	campaignData := make([]map[string]*LocationRecord, len(files))
	for i := range campaignData {
		campaignData[i] = make(map[string]*LocationRecord)
	}

	var wg sync.WaitGroup
	var totalRecordsProcessed atomic.Int64

	for w := 0; w < fileWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			dt.campaignWorker(workerID, fileJobs, campaignData, &totalRecordsProcessed)
		}(w)
	}

	wg.Wait()

	fmt.Printf("✅ Processed %d total records\n", totalRecordsProcessed.Load())

	if err := dt.saveCampaignResults(campaignData, dateStr); err != nil {
		return err
	}

	return nil
}

func (dt *DeviceTracker) campaignWorker(workerID int, jobs <-chan FileJob, campaignData []map[string]*LocationRecord, totalRecords *atomic.Int64) {
	for job := range jobs {
		records, err := dt.readParquetFileRowGroups(job.FilePath)
		if err != nil {
			fmt.Printf("[W%d] Error reading %s: %v\n", workerID, filepath.Base(job.FilePath), err)
			continue
		}

		localCount := 0
		for _, record := range records {
			point := orb.Point{record.Longitude, record.Latitude}
			intersections := dt.findIntersectingLocations(point)

			for _, idx := range intersections {
				dt.LocDataMutex.RLock()
				loc := dt.LocData[idx]
				dt.LocDataMutex.RUnlock()

				key := loc.POIID
				if campaignData[job.Index][key] == nil {
					campaignData[job.Index][key] = &LocationRecord{
						Address:    loc.Address,
						Campaign:   loc.Campaign,
						CampaignID: loc.CampaignID,
						POIID:      loc.POIID,
						Devices:    make([]MinimalDeviceRecord, 0),
					}
				}

				campaignData[job.Index][key].Devices = append(
					campaignData[job.Index][key].Devices,
					MinimalDeviceRecord{
						DeviceID:       record.DeviceID,
						EventTimestamp: record.EventTimestamp,
					},
				)
			}
			localCount++
		}

		totalRecords.Add(int64(localCount))
		fmt.Printf("[W%d] %s: %d records\n", workerID, filepath.Base(job.FilePath), localCount)
	}
}

func (dt *DeviceTracker) saveCampaignResults(campaignData []map[string]*LocationRecord, dateStr string) error {
	merged := make(map[string]*LocationRecord)

	for _, fileData := range campaignData {
		for poiID, locRec := range fileData {
			if merged[poiID] == nil {
				merged[poiID] = &LocationRecord{
					Address:    locRec.Address,
					Campaign:   locRec.Campaign,
					CampaignID: locRec.CampaignID,
					POIID:      locRec.POIID,
					Devices:    make([]MinimalDeviceRecord, 0),
				}
			}
			merged[poiID].Devices = append(merged[poiID].Devices, locRec.Devices...)
		}
	}

	campaignMap := make(map[string]*CampaignDevices)
	for _, locRec := range merged {
		if campaignMap[locRec.CampaignID] == nil {
			campaignMap[locRec.CampaignID] = &CampaignDevices{
				CampaignID:   locRec.CampaignID,
				CampaignName: locRec.Campaign,
				POIs:         make([]POIDevices, 0),
			}
		}

		campaignMap[locRec.CampaignID].POIs = append(campaignMap[locRec.CampaignID].POIs, POIDevices{
			POIID:   locRec.POIID,
			POIName: locRec.Address,
			Devices: locRec.Devices,
			Count:   len(locRec.Devices),
		})
		campaignMap[locRec.CampaignID].TotalDevices += len(locRec.Devices)
	}

	campaigns := make([]CampaignDevices, 0, len(campaignMap))
	totalDevices := 0
	for _, camp := range campaignMap {
		campaigns = append(campaigns, *camp)
		totalDevices += camp.TotalDevices
	}

	output := CampaignIntersectionOutput{
		ProcessedDate:    dateStr,
		TotalDevices:     totalDevices,
		TotalCampaigns:   len(campaigns),
		ProcessingTimeMs: 0,
		Campaigns:        campaigns,
	}

	outputPath := filepath.Join(dt.OutputFolder, fmt.Sprintf("campaign_intersection_%s.json", strings.ReplaceAll(dateStr, "-", "")))
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		return err
	}

	fmt.Printf("✅ Saved campaign intersection: %d campaigns, %d devices\n", len(campaigns), totalDevices)

	return dt.saveToMongoDB(campaigns, dateStr)
}

func (dt *DeviceTracker) saveToMongoDB(campaigns []CampaignDevices, dateStr string) error {
	for _, campaign := range campaigns {
		for _, poi := range campaign.POIs {
			poiObjID, err := primitive.ObjectIDFromHex(poi.POIID)
			if err != nil {
				continue
			}

			doc := POIMongoDocument{
				POIID:         poiObjID,
				POIName:       poi.POIName,
				ProcessedDate: dateStr,
				DeviceCount:   poi.Count,
				Devices:       poi.Devices,
				CreatedAt:     time.Now(),
			}

			_, err = dt.mongoCollection.InsertOne(dt.ctx, doc)
			if err != nil {
				fmt.Printf("⚠️  MongoDB insert error for POI %s: %v\n", poi.POIID, err)
			}
		}
	}

	fmt.Printf("✅ Saved to MongoDB\n")
	return nil
}

// ============================================================================
// STEP 2: TIME FILTERING WITH TIMEZONE-AWARE PARQUET
// ============================================================================

// 🆕 MODIFIED: Use source timezone for schema
func (dt *DeviceTracker) initParquetWriters(loadDate string) error {
	dt.parquetWriterMutex.Lock()
	defer dt.parquetWriterMutex.Unlock()

	dt.parquetWriters = make([]*ParquetWriter, dt.numWriters)

	outputDir := filepath.Join(dt.OutputFolder, "time_filtered")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// 🔧 CRITICAL: Use source timezone to preserve local time
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "device_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "event_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone}, Nullable: false},
		{Name: "latitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "longitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "load_date", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone}, Nullable: false},
	}, nil)

	for i := 0; i < dt.numWriters; i++ {
		filename := filepath.Join(outputDir, fmt.Sprintf("time_filtered_part_%s_%02d.parquet", strings.ReplaceAll(loadDate, "-", ""), i))

		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create parquet file: %w", err)
		}

		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
			parquet.WithDictionaryDefault(true),
		)

		arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

		writer, err := pqarrow.NewFileWriter(
			schema,
			file,
			writerProps,
			arrowProps,
		)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to create arrow writer: %w", err)
		}

		dt.parquetWriters[i] = &ParquetWriter{
			writer: writer,
			file:   file,
			batch:  make([]TimeFilteredRecord, 0, parquetBatchSize),
			schema: schema,
			date:   loadDate,
			id:     i,
		}
	}

	return nil
}

func (dt *DeviceTracker) processTimeFiltering(folderPath, dateStr string) error {
	files, err := filepath.Glob(filepath.Join(folderPath, "*.parquet"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no parquet files found")
	}

	fmt.Printf("📂 Processing %d files for time filtering\n", len(files))

	jobs := make(chan string, len(files))
	for _, f := range files {
		jobs <- f
	}
	close(jobs)

	var wg sync.WaitGroup
	var filteredIn atomic.Int64

	for w := 0; w < dt.numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			dt.timeFilterWorker(workerID, jobs, dateStr, &filteredIn)
		}(w)
	}

	wg.Wait()

	fmt.Printf("✅ Filtered %d records\n", filteredIn.Load())
	return nil
}

func (dt *DeviceTracker) timeFilterWorker(workerID int, jobs <-chan string, loadDate string, filteredIn *atomic.Int64) {
	loadDateTime, _ := time.Parse("2006-01-02", loadDate)

	for filePath := range jobs {
		records, err := dt.readParquetFileRowGroups(filePath)
		if err != nil {
			fmt.Printf("[W%d] Error: %v\n", workerID, err)
			continue
		}

		batch := make([]TimeFilteredRecord, 0)
		for _, rec := range records {
			// 🔧 CRITICAL: Filter based on LOCAL time (hour, minute, second)
			// NOT converting timezone - keeping original local time
			eventTime := rec.EventTimestamp
			seconds := eventTime.Hour()*3600 + eventTime.Minute()*60 + eventTime.Second()

			if seconds >= dt.startSeconds && seconds <= dt.endSeconds {
				batch = append(batch, TimeFilteredRecord{
					DeviceID:       rec.DeviceID,
					EventTimestamp: rec.EventTimestamp, // Preserve original timestamp with timezone
					Latitude:       rec.Latitude,
					Longitude:      rec.Longitude,
					LoadDate:       loadDateTime,
				})
			}
		}

		if len(batch) > 0 {
			dt.addToParquetBatch(workerID, batch)
			filteredIn.Add(int64(len(batch)))
		}

		fmt.Printf("[W%d] %s: %d filtered\n", workerID, filepath.Base(filePath), len(batch))
	}

	dt.flushParquetBatch(workerID)
}

func (dt *DeviceTracker) addToParquetBatch(writerID int, records []TimeFilteredRecord) {
	writer := dt.parquetWriters[writerID]
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	writer.batch = append(writer.batch, records...)

	if len(writer.batch) >= parquetBatchSize {
		dt.writeParquetBatch(writer)
		writer.batch = make([]TimeFilteredRecord, 0, parquetBatchSize)
	}
}

func (dt *DeviceTracker) flushParquetBatch(writerID int) {
	writer := dt.parquetWriters[writerID]
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	if len(writer.batch) > 0 {
		dt.writeParquetBatch(writer)
		writer.batch = nil
	}
}

// 🔧 Use source timezone in builders
func (dt *DeviceTracker) writeParquetBatch(pw *ParquetWriter) {
	if len(pw.batch) == 0 {
		return
	}

	deviceIDBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	timestampBuilder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone})
	latBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	lonBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	loadDateBuilder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone})

	for _, rec := range pw.batch {
		deviceIDBuilder.Append(rec.DeviceID)
		timestampBuilder.Append(arrow.Timestamp(rec.EventTimestamp.UnixMicro()))
		latBuilder.Append(rec.Latitude)
		lonBuilder.Append(rec.Longitude)
		loadDateBuilder.Append(arrow.Timestamp(rec.LoadDate.UnixMicro()))
	}

	deviceIDArr := deviceIDBuilder.NewArray()
	timestampArr := timestampBuilder.NewArray()
	latArr := latBuilder.NewArray()
	lonArr := lonBuilder.NewArray()
	loadDateArr := loadDateBuilder.NewArray()

	record := array.NewRecord(pw.schema, []arrow.Array{
		deviceIDArr,
		timestampArr,
		latArr,
		lonArr,
		loadDateArr,
	}, int64(len(pw.batch)))

	if err := pw.writer.Write(record); err != nil {
		fmt.Printf("Error writing parquet: %v\n", err)
	}

	record.Release()
	deviceIDArr.Release()
	timestampArr.Release()
	latArr.Release()
	lonArr.Release()
	loadDateArr.Release()
}

func (dt *DeviceTracker) closeParquetWriters() {
	dt.parquetWriterMutex.Lock()
	defer dt.parquetWriterMutex.Unlock()

	for _, writer := range dt.parquetWriters {
		if writer != nil {
			writer.writer.Close()
			writer.file.Close()
		}
	}
}

// ============================================================================
// MERGE BY EVENT DATE
// ============================================================================

func (dt *DeviceTracker) mergeParquetFilesByEventDate(loadDate string) error {
	timeFilteredDir := filepath.Join(dt.OutputFolder, "time_filtered")

	partPattern := filepath.Join(timeFilteredDir,
		fmt.Sprintf("time_filtered_part_%s_*.parquet", strings.ReplaceAll(loadDate, "-", "")))
	partFiles, err := filepath.Glob(partPattern)
	if err != nil || len(partFiles) == 0 {
		fmt.Println("⚠️  No part files to merge")
		return nil
	}

	fmt.Printf("\n📦 Merging %d part files by event date...\n", len(partFiles))

	recordsByEventDate := make(map[string][]TimeFilteredRecord)

	for _, partFile := range partFiles {
		records, err := dt.readTimeFilteredParquet(partFile)
		if err != nil {
			fmt.Printf("⚠️  Warning: could not read %s: %v\n", filepath.Base(partFile), err)
			continue
		}

		for _, rec := range records {
			// 🔧 Use local date (not UTC date) for grouping
			eventDate := rec.EventTimestamp.Format("20060102")
			recordsByEventDate[eventDate] = append(recordsByEventDate[eventDate], rec)
		}

		os.Remove(partFile)
	}

	for eventDate, records := range recordsByEventDate {
		outputFile := filepath.Join(timeFilteredDir,
			fmt.Sprintf("time_filtered_%s.parquet", eventDate))

		if _, err := os.Stat(outputFile); err == nil {
			fmt.Printf("   📝 Appending %d records to time_filtered_%s.parquet\n",
				len(records), eventDate)
			existingRecords, _ := dt.readTimeFilteredParquet(outputFile)
			records = append(existingRecords, records...)
		} else {
			fmt.Printf("   ✨ Creating time_filtered_%s.parquet (%d records)\n",
				eventDate, len(records))
		}

		if err := dt.writeTimeFilteredParquet(outputFile, records); err != nil {
			return fmt.Errorf("failed to write %s: %w", filepath.Base(outputFile), err)
		}
	}

	fmt.Println("✅ Merge complete")
	return nil
}

func (dt *DeviceTracker) readTimeFilteredParquet(filePath string) ([]TimeFilteredRecord, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader,
		pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, err
	}

	recordReader, err := arrowReader.GetRecordReader(context.TODO(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer recordReader.Release()

	var allRecords []TimeFilteredRecord

	for recordReader.Next() {
		rec := recordReader.Record()
		rows := int(rec.NumRows())

		for i := 0; i < rows; i++ {
			record := TimeFilteredRecord{}

			if col, ok := rec.Column(0).(*array.String); ok {
				record.DeviceID = col.Value(i)
			}
			if col, ok := rec.Column(1).(*array.Timestamp); ok {
				// Timestamp preserves timezone
				record.EventTimestamp = col.Value(i).ToTime(arrow.Microsecond)
			}
			if col, ok := rec.Column(2).(*array.Float64); ok {
				record.Latitude = col.Value(i)
			}
			if col, ok := rec.Column(3).(*array.Float64); ok {
				record.Longitude = col.Value(i)
			}
			if col, ok := rec.Column(4).(*array.Timestamp); ok {
				record.LoadDate = col.Value(i).ToTime(arrow.Microsecond)
			}

			allRecords = append(allRecords, record)
		}
		rec.Release()
	}

	return allRecords, nil
}

// 🔧 Use source timezone in write function
func (dt *DeviceTracker) writeTimeFilteredParquet(filePath string, records []TimeFilteredRecord) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "device_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "event_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone}, Nullable: false},
		{Name: "latitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "longitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "load_date", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone}, Nullable: false},
	}, nil)

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(schema, file, writerProps, arrowProps)
	if err != nil {
		return err
	}
	defer writer.Close()

	batchSize := 100000
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		deviceIDBuilder := array.NewStringBuilder(memory.DefaultAllocator)
		timestampBuilder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone})
		latBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
		lonBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
		loadDateBuilder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: dt.sourceTimezone})

		for _, rec := range batch {
			deviceIDBuilder.Append(rec.DeviceID)
			timestampBuilder.Append(arrow.Timestamp(rec.EventTimestamp.UnixMicro()))
			latBuilder.Append(rec.Latitude)
			lonBuilder.Append(rec.Longitude)
			loadDateBuilder.Append(arrow.Timestamp(rec.LoadDate.UnixMicro()))
		}

		deviceIDArr := deviceIDBuilder.NewArray()
		timestampArr := timestampBuilder.NewArray()
		latArr := latBuilder.NewArray()
		lonArr := lonBuilder.NewArray()
		loadDateArr := loadDateBuilder.NewArray()

		record := array.NewRecord(schema, []arrow.Array{
			deviceIDArr,
			timestampArr,
			latArr,
			lonArr,
			loadDateArr,
		}, int64(len(batch)))

		if err := writer.Write(record); err != nil {
			return err
		}

		record.Release()
		deviceIDArr.Release()
		timestampArr.Release()
		latArr.Release()
		lonArr.Release()
		loadDateArr.Release()
	}

	return nil
}

// ============================================================================
// STEP 3: IDLE DEVICE DETECTION
// ============================================================================

func (dt *DeviceTracker) RunIdleDeviceSearch(folderList, dates []string) error {
	fmt.Printf("\n🚀 STEP 3: Idle Device Detection - %d WORKERS\n", step3Workers)
	startTime := time.Now()

	allIdleDevicesByDate := make(map[string][]IdleDeviceResult)

	for idx := range folderList {
		loadDate := dates[idx]
		fmt.Printf("\n📅 Processing %s\n", loadDate)

		timeFilteredDir := filepath.Join(dt.OutputFolder, "time_filtered")
		eventDate := strings.ReplaceAll(loadDate, "-", "")
		timeFilteredFile := filepath.Join(timeFilteredDir, fmt.Sprintf("time_filtered_%s.parquet", eventDate))

		if _, err := os.Stat(timeFilteredFile); os.IsNotExist(err) {
			fmt.Printf("⚠️  Time filtered file not found: %s\n", timeFilteredFile)
			continue
		}

		idleDevices, err := dt.findIdleDevices(timeFilteredFile, loadDate)
		if err != nil {
			return err
		}

		allIdleDevicesByDate[loadDate] = idleDevices
		fmt.Printf("✅ Found %d idle devices for %s\n", len(idleDevices), loadDate)
	}

	elapsed := time.Since(startTime)
	totalIdle := 0
	for _, devices := range allIdleDevicesByDate {
		totalIdle += len(devices)
	}

	fmt.Printf("\n✅ Total idle devices across all dates: %d\n", totalIdle)
	fmt.Printf("⏱️  Processing time: %.2fs\n", elapsed.Seconds())

	return dt.saveIdleDevicesSeparateFiles(allIdleDevicesByDate, elapsed)
}

func (dt *DeviceTracker) saveIdleDevicesSeparateFiles(allIdleDevicesByDate map[string][]IdleDeviceResult, elapsed time.Duration) error {
	idleDevicesDir := filepath.Join(dt.OutputFolder, "idle_devices")
	if err := os.MkdirAll(idleDevicesDir, 0755); err != nil {
		return fmt.Errorf("failed to create idle_devices directory: %w", err)
	}

	fmt.Println("\n📁 Saving idle devices to separate files...")

	for loadDate, devices := range allIdleDevicesByDate {
		output := IdleDevicesByDate{
			EventDate:        loadDate,
			TotalIdleDevices: len(devices),
			IdleDevices:      devices,
		}

		outputPath := filepath.Join(idleDevicesDir,
			fmt.Sprintf("idle_devices_%s.json", loadDate))

		file, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create file for %s: %w", loadDate, err)
		}

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			file.Close()
			return fmt.Errorf("failed to encode for %s: %w", loadDate, err)
		}
		file.Close()

		fmt.Printf("✅ Saved idle_devices_%s.json: %d devices\n", loadDate, len(devices))
	}

	fmt.Printf("\n✅ All idle device files saved to: %s\n", idleDevicesDir)
	return nil
}

func (dt *DeviceTracker) findIdleDevices(timeFilteredFile, loadDate string) ([]IdleDeviceResult, error) {
	records, err := dt.readTimeFilteredParquet(timeFilteredFile)
	if err != nil {
		return nil, err
	}

	fmt.Printf("   Loaded %d time-filtered records\n", len(records))

	deviceRecords := make(map[string][]TimeFilteredRecord)
	for _, rec := range records {
		deviceRecords[rec.DeviceID] = append(deviceRecords[rec.DeviceID], rec)
	}

	for deviceID := range deviceRecords {
		sort.Slice(deviceRecords[deviceID], func(i, j int) bool {
			return deviceRecords[deviceID][i].EventTimestamp.Before(deviceRecords[deviceID][j].EventTimestamp)
		})
	}

	idleDevices := make([]IdleDeviceResult, 0)
	processedCount := 0

	for deviceID, recs := range deviceRecords {
		processedCount++
		if processedCount%10000 == 0 {
			fmt.Printf("   Processed %d/%d devices...\n", processedCount, len(deviceRecords))
		}

		if len(recs) < 2 {
			continue
		}

		firstRec := recs[0]
		firstPoint := orb.Point{firstRec.Longitude, firstRec.Latitude}
		isIdle := true

		for i := 1; i < len(recs); i++ {
			currentPoint := orb.Point{recs[i].Longitude, recs[i].Latitude}
			distance := geo.Distance(firstPoint, currentPoint)

			if distance > dt.IdleDeviceBuffer {
				isIdle = false
				break
			}
		}

		if isIdle {
			intersections := dt.findIntersectingLocations(firstPoint)
			if len(intersections) > 0 {
				dt.LocDataMutex.RLock()
				loc := dt.LocData[intersections[0]]
				dt.LocDataMutex.RUnlock()

				idleDevices = append(idleDevices, IdleDeviceResult{
					DeviceID:    deviceID,
					VisitedTime: firstRec.EventTimestamp.Format(time.RFC3339),
					Address:     loc.Address,
					Campaign:    loc.Campaign,
					CampaignID:  loc.CampaignID,
					POIID:       loc.POIID,
					Geometry:    fmt.Sprintf("POINT (%f %f)", firstRec.Longitude, firstRec.Latitude),
				})
			}
		}
	}

	return idleDevices, nil
}

// ============================================================================
// PARQUET READING HELPERS
// ============================================================================

func (dt *DeviceTracker) readParquetFileRowGroups(fpath string) ([]DeviceRecord, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	numRowGroups := reader.NumRowGroups()
	allRecords := make([]DeviceRecord, 0)

	for rg := 0; rg < numRowGroups; rg++ {
		rgReader := reader.RowGroup(rg)
		records, err := dt.readRowGroupRecords(rgReader)
		if err != nil {
			continue
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

func (dt *DeviceTracker) readRowGroupRecords(rg *file.RowGroupReader) ([]DeviceRecord, error) {
	numRows := int(rg.NumRows())
	if numRows == 0 {
		return nil, nil
	}

	schema := rg.MetaData().Schema

	colIndices := make(map[string]int)
	for i := 0; i < schema.NumColumns(); i++ {
		colIndices[schema.Column(i).Name()] = i
	}

	deviceIDIdx, hasDeviceID := colIndices[dt.DeviceIDColumn]
	timestampIdx, hasTimestamp := colIndices[dt.TimeColumnName]
	latIdx, hasLat := colIndices[dt.LatColumn]
	lonIdx, hasLon := colIndices[dt.LonColumn]

	if !hasDeviceID || !hasTimestamp || !hasLat || !hasLon {
		return nil, fmt.Errorf("missing required columns")
	}

	deviceIDs := dt.readStringColumn(rg, deviceIDIdx, numRows)
	timestamps := dt.readTimestampColumn(rg, timestampIdx, numRows)
	latitudes := dt.readFloatColumn(rg, latIdx, numRows)
	longitudes := dt.readFloatColumn(rg, lonIdx, numRows)

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
		maxDefLevel := reader.Descriptor().MaxDefinitionLevel()

		for {
			n, _, _ := reader.ReadBatch(int64(len(values)), values, defLevels, nil)
			if n == 0 {
				break
			}

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
	/*yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	dates := []string{
		yesterday,
	}*/

	dates := GetLastNDatesFromYesterday(7)

	folderList := make([]string, 0, len(dates))
	for _, d := range dates {
		folder := "load_date=" + strings.ReplaceAll(d, "-", "")
		folderList = append(folderList, folder)
	}

	fmt.Printf("📅 Dates: %v\n", dates)

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
		for _, folder := range folderList {
			err := dt.FindCampaignIntersectionForFolder("/mnt/blobcontainer/"+folder, step1, step2)
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			}
		}
	}

	if step3 {
		err := dt.RunIdleDeviceSearch(folderList, dates)
		if err != nil {
			log.Fatal(err)
		}
	}

	if step4 {
		yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

		consumerFolder := filepath.Join(outputFolder, "consumers")
		idleDevicesPath := filepath.Join(outputFolder, "idle_devices",
			fmt.Sprintf("idle_devices_%s.json", yesterday))

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
	fmt.Println("║     DEVICE TRACKER - TIMEZONE-AWARE EDITION          ║")
	fmt.Println("║     48 CORES | 386GB RAM | LOCAL TIME FILTERING      ║")
	fmt.Println("╚═══════════════════════════════════════════════════════╝")

	startTime := time.Now()

	runSteps := []int{1, 2, 3}

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
