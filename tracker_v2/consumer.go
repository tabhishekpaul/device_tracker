package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

const (
	MatchRadiusMeters = 30.0 // Buffer distance in meters (like Python's buffer=20)
	ParallelWorkers   = 12   // Number of parallel workers for processing parquet files
)

// ============================================================================
// TYPES
// ============================================================================

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

type IdleDevice struct {
	DeviceID    string
	VisitedTime string
	Address     string
	Campaign    string
	CampaignID  string
	POIID       string
	Latitude    float64
	Longitude   float64
	Geometry    orb.Polygon
}

type ConsumerDeviceMatch struct {
	// Device info
	DeviceID    string `json:"DeviceID"`
	DateVisited string `json:"Date visited"`
	TimeVisited string `json:"Time visited"`

	// Consumer info
	ConsumerID int64  `json:"ConsumerID"`
	Name       string `json:"Name"`
	Address    string `json:"Address"`
	Email      string `json:"Email"`
	CityName   string `json:"CityName"`
	State      string `json:"State"`
	ZipCode    string `json:"ZipCode"`

	// POI/Campaign info
	POI      string `json:"POI"`
	Campaign string `json:"Campaign"`
}

type MatchOutputJSON struct {
	ProcessedDate    string                `json:"processed_date"`
	TotalMatches     int                   `json:"total_matches"`
	UniqueDevices    int                   `json:"unique_devices"`
	UniqueConsumers  int                   `json:"unique_consumers"`
	ProcessingTimeMs int64                 `json:"processing_time_ms"`
	Matches          []ConsumerDeviceMatch `json:"matches"`
}

type ConsumerDeviceMatcher struct {
	outputFolder    string
	consumerFolder  string
	idleDevicesPath string
	logger          *log.Logger

	// Idle devices loaded once
	idleDevices []IdleDevice

	// Results collection
	matches      []ConsumerDeviceMatch
	matchesMutex sync.Mutex

	// Tracking
	totalMatches      atomic.Int64
	processedFiles    atomic.Int64
	uniqueDeviceSet   map[string]bool
	uniqueConsumerSet map[int64]bool
}

func NewConsumerDeviceMatcher(outputFolder, consumerFolder, idleDevicesPath string) *ConsumerDeviceMatcher {
	return &ConsumerDeviceMatcher{
		outputFolder:      outputFolder,
		consumerFolder:    consumerFolder,
		idleDevicesPath:   idleDevicesPath,
		logger:            log.New(os.Stdout, "[Step4] ", log.LstdFlags),
		matches:           make([]ConsumerDeviceMatch, 0),
		uniqueDeviceSet:   make(map[string]bool),
		uniqueConsumerSet: make(map[int64]bool),
	}
}

// ============================================================================
// LOAD IDLE DEVICES
// ============================================================================

func (cdm *ConsumerDeviceMatcher) loadIdleDevices() error {
	cdm.logger.Println("Loading idle devices from JSON...")

	file, err := os.Open(cdm.idleDevicesPath)
	if err != nil {
		return fmt.Errorf("failed to open idle devices file: %w", err)
	}
	defer file.Close()

	var idleData struct {
		IdleDevicesByDate map[string][]struct {
			DeviceID    string `json:"device_id"`
			VisitedTime string `json:"visited_time"`
			Address     string `json:"address"`
			Campaign    string `json:"campaign"`
			CampaignID  string `json:"campaign_id"`
			POIID       string `json:"poi_id"`
			Geometry    string `json:"geometry"`
		} `json:"idle_devices_by_date"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&idleData); err != nil {
		return fmt.Errorf("failed to decode idle devices: %w", err)
	}

	// Flatten all dates into single list
	cdm.idleDevices = make([]IdleDevice, 0)

	for date, devices := range idleData.IdleDevicesByDate {
		for _, device := range devices {
			// Parse geometry "POINT (lon lat)"
			lat, lon := cdm.parsePoint(device.Geometry)

			// Create buffer polygon (circle approximation with 16 points)
			polygon := cdm.createBufferPolygon(lat, lon, MatchRadiusMeters)

			cdm.idleDevices = append(cdm.idleDevices, IdleDevice{
				DeviceID:    device.DeviceID,
				VisitedTime: device.VisitedTime,
				Address:     device.Address,
				Campaign:    device.Campaign,
				CampaignID:  device.CampaignID,
				POIID:       device.POIID,
				Latitude:    lat,
				Longitude:   lon,
				Geometry:    polygon,
			})
		}
		cdm.logger.Printf("  Loaded %d devices from date %s", len(devices), date)
	}

	cdm.logger.Printf("✅ Total idle devices loaded: %d", len(cdm.idleDevices))
	return nil
}

func (cdm *ConsumerDeviceMatcher) parsePoint(geometryStr string) (float64, float64) {
	// Parse "POINT (lon lat)"
	geometryStr = strings.TrimPrefix(geometryStr, "POINT (")
	geometryStr = strings.TrimSuffix(geometryStr, ")")

	var lon, lat float64
	fmt.Sscanf(geometryStr, "%f %f", &lon, &lat)
	return lat, lon
}

func (cdm *ConsumerDeviceMatcher) createBufferPolygon(lat, lon, radiusMeters float64) orb.Polygon {
	// Create a circle approximation with 16 points
	const numPoints = 16
	ring := make(orb.Ring, numPoints+1)

	// Convert radius from meters to degrees (approximate)
	radiusLat := radiusMeters / 111000.0                 // 1 degree latitude ≈ 111km
	radiusLon := radiusMeters / (111000.0 * cosDeg(lat)) // Adjust for latitude

	for i := 0; i < numPoints; i++ {
		angle := float64(i) * 2.0 * 3.14159265359 / float64(numPoints)
		x := lon + radiusLon*cos(angle)
		y := lat + radiusLat*sin(angle)
		ring[i] = orb.Point{x, y}
	}
	ring[numPoints] = ring[0] // Close the ring

	return orb.Polygon{ring}
}

// ============================================================================
// PROCESS CONSUMER PARQUET FILES IN PARALLEL
// ============================================================================

func (cdm *ConsumerDeviceMatcher) processConsumerFiles() error {
	cdm.logger.Println("Finding consumer parquet files...")

	pattern := filepath.Join(cdm.consumerFolder, "*.parquet")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no consumer parquet files found in %s", cdm.consumerFolder)
	}

	cdm.logger.Printf("Found %d consumer parquet files", len(files))
	cdm.logger.Printf("Processing with %d parallel workers...\n", ParallelWorkers)

	// Channel for file jobs
	fileChan := make(chan string, len(files))
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	// Worker pool
	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < ParallelWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for fpath := range fileChan {
				fileStartTime := time.Now()

				matches, err := cdm.processConsumerFile(fpath)
				if err != nil {
					cdm.logger.Printf("[W%d] Error processing %s: %v", workerID, filepath.Base(fpath), err)
					continue
				}

				// Add matches to global list
				if len(matches) > 0 {
					cdm.matchesMutex.Lock()
					cdm.matches = append(cdm.matches, matches...)
					cdm.matchesMutex.Unlock()

					cdm.totalMatches.Add(int64(len(matches)))
				}

				cdm.processedFiles.Add(1)
				elapsed := time.Since(fileStartTime)

				cdm.logger.Printf("[W%d] %s: %d matches (%.1fs) | Total: %d files, %d matches",
					workerID, filepath.Base(fpath), len(matches), elapsed.Seconds(),
					cdm.processedFiles.Load(), cdm.totalMatches.Load())

				runtime.GC()
			}
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	cdm.logger.Printf("\n✅ All files processed in %.2f seconds", elapsed.Seconds())
	cdm.logger.Printf("   Total matches found: %d", cdm.totalMatches.Load())

	return nil
}

// ============================================================================
// PROCESS SINGLE CONSUMER PARQUET FILE (SPATIAL JOIN)
// ============================================================================

func (cdm *ConsumerDeviceMatcher) processConsumerFile(fpath string) ([]ConsumerDeviceMatch, error) {
	// Read consumers from parquet
	consumers, err := cdm.readConsumersFromParquet(fpath)
	if err != nil {
		return nil, err
	}

	if len(consumers) == 0 {
		return nil, nil
	}

	matches := make([]ConsumerDeviceMatch, 0)
	deviceMatched := make(map[string]bool) // Track matched devices to avoid duplicates

	// For each idle device, check intersection with consumers
	for _, device := range cdm.idleDevices {
		// Skip if already matched
		if deviceMatched[device.DeviceID] {
			continue
		}

		devicePoint := orb.Point{device.Longitude, device.Latitude}

		// Check each consumer
		for _, consumer := range consumers {
			consumerPoint := orb.Point{consumer.Longitude, consumer.Latitude}

			// Calculate distance
			distance := geo.Distance(devicePoint, consumerPoint)

			// Check if within buffer (30 meters)
			if distance <= MatchRadiusMeters {
				// Parse visited time
				datePart, timePart := cdm.splitDateTime(device.VisitedTime)

				match := ConsumerDeviceMatch{
					DeviceID:    device.DeviceID,
					DateVisited: datePart,
					TimeVisited: timePart,
					ConsumerID:  cdm.parseConsumerID(consumer.ID),
					Name:        cdm.buildName(consumer.PersonFirstName, consumer.PersonLastName),
					Address:     consumer.PrimaryAddress,
					Email:       consumer.Email,
					CityName:    consumer.CityName,
					State:       consumer.State,
					ZipCode:     consumer.ZipCode,
					POI:         device.Address,
					Campaign:    device.Campaign,
				}

				matches = append(matches, match)
				deviceMatched[device.DeviceID] = true
				break // Only first match per device (like drop_duplicates)
			}
		}
	}

	return matches, nil
}

// ============================================================================
// READ CONSUMERS FROM PARQUET
// ============================================================================

func (cdm *ConsumerDeviceMatcher) readConsumersFromParquet(fpath string) ([]ConsumerRecord, error) {
	pf, err := file.OpenParquetFile(fpath, false)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	schema := pf.MetaData().Schema
	colIndex := make(map[string]int)
	for i := 0; i < schema.NumColumns(); i++ {
		colIndex[schema.Column(i).Name()] = i
	}

	consumers := make([]ConsumerRecord, 0)

	numRowGroups := pf.NumRowGroups()
	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		rg := pf.RowGroup(rgIdx)
		numRows := int(rg.NumRows())

		// Read all columns
		ids := cdm.readStringColumnRaw(rg, colIndex["id"], numRows)
		lats := cdm.readFloatColumnRaw(rg, colIndex["Latitude"], numRows)
		lons := cdm.readFloatColumnRaw(rg, colIndex["Longitude"], numRows)
		firstNames := cdm.readStringColumnRaw(rg, colIndex["PersonFirstName"], numRows)
		lastNames := cdm.readStringColumnRaw(rg, colIndex["PersonLastName"], numRows)
		addresses := cdm.readStringColumnRaw(rg, colIndex["PrimaryAddress"], numRows)
		phones := cdm.readStringColumnRaw(rg, colIndex["TenDigitPhone"], numRows)
		emails := cdm.readStringColumnRaw(rg, colIndex["Email"], numRows)
		cities := cdm.readStringColumnRaw(rg, colIndex["CityName"], numRows)
		states := cdm.readStringColumnRaw(rg, colIndex["State"], numRows)
		zips := cdm.readStringColumnRaw(rg, colIndex["ZipCode"], numRows)

		log.Println(lats, lons)

		// Build records
		for i := 0; i < numRows; i++ {
			// Validate coordinates
			if lats[i] < -90 || lats[i] > 90 || lons[i] < -180 || lons[i] > 180 {
				continue
			}

			consumers = append(consumers, ConsumerRecord{
				ID:              ids[i],
				Latitude:        lats[i],
				Longitude:       lons[i],
				PersonFirstName: firstNames[i],
				PersonLastName:  lastNames[i],
				PrimaryAddress:  addresses[i],
				TenDigitPhone:   phones[i],
				Email:           emails[i],
				CityName:        cities[i],
				State:           states[i],
				ZipCode:         zips[i],
			})
		}
	}

	return consumers, nil
}

// ============================================================================
// PARQUET COLUMN READERS
// ============================================================================

func (cdm *ConsumerDeviceMatcher) readStringColumnRaw(rg *file.RowGroupReader, colIdx int, numRows int) []string {
	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]string, numRows)
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

func (cdm *ConsumerDeviceMatcher) readFloatColumnRaw(rg *file.RowGroupReader, colIdx int, numRows int) []float64 {
	col, err := rg.Column(colIdx)
	if err != nil {
		return make([]float64, numRows)
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
	}

	return result
}

// ============================================================================
// SAVE RESULTS
// ============================================================================

func (cdm *ConsumerDeviceMatcher) saveMatches(processedDate string, processingTimeMs int64) error {
	cdm.logger.Println("\nSaving matches...")

	// Calculate unique counts
	for _, match := range cdm.matches {
		cdm.uniqueDeviceSet[match.DeviceID] = true
		cdm.uniqueConsumerSet[match.ConsumerID] = true
	}

	output := MatchOutputJSON{
		ProcessedDate:    processedDate,
		TotalMatches:     len(cdm.matches),
		UniqueDevices:    len(cdm.uniqueDeviceSet),
		UniqueConsumers:  len(cdm.uniqueConsumerSet),
		ProcessingTimeMs: processingTimeMs,
		Matches:          cdm.matches,
	}

	// Save JSON
	outputPath := filepath.Join(cdm.outputFolder, "consumer_device_matches", fmt.Sprintf("matches_%s.json", processedDate))
	os.MkdirAll(filepath.Dir(outputPath), 0755)

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

	cdm.logger.Printf("✅ Saved to: %s", outputPath)
	cdm.logger.Printf("   Total matches: %d", output.TotalMatches)
	cdm.logger.Printf("   Unique devices: %d", output.UniqueDevices)
	cdm.logger.Printf("   Unique consumers: %d", output.UniqueConsumers)

	return nil
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func (cdm *ConsumerDeviceMatcher) splitDateTime(visitedTime string) (string, string) {
	parts := strings.Split(visitedTime, " ")
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	return visitedTime, ""
}

func (cdm *ConsumerDeviceMatcher) buildName(firstName, lastName string) string {
	if firstName == "" && lastName == "" {
		return ""
	}
	return strings.TrimSpace(firstName + " " + lastName)
}

func (cdm *ConsumerDeviceMatcher) parseConsumerID(idStr string) int64 {
	var id int64
	fmt.Sscanf(idStr, "%d", &id)
	return id
}

// Math helpers
func cosDeg(degrees float64) float64 {
	return cos(degrees * 3.14159265359 / 180.0)
}

func cos(x float64) float64 {
	// Taylor series approximation
	return 1 - x*x/2 + x*x*x*x/24
}

func sin(x float64) float64 {
	// Taylor series approximation
	return x - x*x*x/6 + x*x*x*x*x/120
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

func (cdm *ConsumerDeviceMatcher) Run() error {
	cdm.logger.Println("╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  STEP 4: CONSUMER-DEVICE MATCHING                    ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

	startTime := time.Now()

	// Step 1: Load idle devices
	if err := cdm.loadIdleDevices(); err != nil {
		return fmt.Errorf("failed to load idle devices: %w", err)
	}

	// Step 2: Process consumer parquet files in parallel
	if err := cdm.processConsumerFiles(); err != nil {
		return fmt.Errorf("failed to process consumer files: %w", err)
	}

	elapsed := time.Since(startTime)

	// Step 3: Save results
	processedDate := time.Now().Format("2006-01-02")
	if err := cdm.saveMatches(processedDate, elapsed.Milliseconds()); err != nil {
		return fmt.Errorf("failed to save matches: %w", err)
	}

	cdm.logger.Printf("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Printf("║  ✅ STEP 4 COMPLETED IN %.2f seconds", elapsed.Seconds())
	cdm.logger.Printf("╚═══════════════════════════════════════════════════════╝")

	return nil
}
