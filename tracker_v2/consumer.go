package main

import (
	"context"
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

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

const (
	MatchRadiusMeters = 30.0
	ParallelWorkers   = 48
	GridSize          = 0.001
)

type ConsumerRecord struct {
	ID              uint64
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
	Latitude    float64
	Longitude   float64
	CampaignID  string
	POIID       string
}

type ConsumerDeviceMatch struct {
	DeviceID    string `json:"DeviceID"`
	DateVisited string `json:"Date visited"`
	TimeVisited string `json:"Time visited"`
	ConsumerID  uint64 `json:"ConsumerID"`
	Name        string `json:"Name"`
	Address     string `json:"Address"`
	Email       string `json:"Email"`
	CityName    string `json:"CityName"`
	State       string `json:"State"`
	ZipCode     string `json:"ZipCode"`
	POI         string `json:"POI"`
	Campaign    string `json:"Campaign"`
	Phone       string `json:"Phone"`
	CampaignID  string `json:"CampaignID"`
	POIID       string `json:"POIID"`
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
	outputFolder       string
	consumerFolder     string
	idleDevicesPath    string
	logger             *log.Logger
	deviceSpatialIndex map[int64][]IdleDevice
	indexMutex         sync.RWMutex
	matches            []ConsumerDeviceMatch
	matchesMutex       sync.Mutex
	matchedDevices     map[string]bool
	totalMatches       atomic.Int64
	processedFiles     atomic.Int64
	processedDate      string
	validConsumers     atomic.Int64
	invalidConsumers   atomic.Int64
	uniqueDeviceSet    map[string]bool
	uniqueConsumerSet  map[uint64]bool
}

func NewConsumerDeviceMatcher(outputFolder, consumerFolder, idleDevicesPath string, processedDate string) *ConsumerDeviceMatcher {
	return &ConsumerDeviceMatcher{
		outputFolder:       outputFolder,
		processedDate:      processedDate,
		consumerFolder:     consumerFolder,
		idleDevicesPath:    idleDevicesPath,
		logger:             log.New(os.Stdout, "[Step4] ", log.LstdFlags),
		deviceSpatialIndex: make(map[int64][]IdleDevice),
		matches:            make([]ConsumerDeviceMatch, 0),
		matchedDevices:     make(map[string]bool),
		uniqueDeviceSet:    make(map[string]bool),
		uniqueConsumerSet:  make(map[uint64]bool),
	}
}

func getCellKey(lat, lon float64) int64 {
	cellX := int64(lon / GridSize)
	cellY := int64(lat / GridSize)
	return cellX*1000000 + cellY
}

func getNearbyCells(lat, lon, radiusMeters float64) []int64 {
	searchRadius := int((radiusMeters/111000.0)/GridSize) + 1
	centerX := int64(lon / GridSize)
	centerY := int64(lat / GridSize)

	cells := make([]int64, 0)
	for dx := -searchRadius; dx <= searchRadius; dx++ {
		for dy := -searchRadius; dy <= searchRadius; dy++ {
			cellX := centerX + int64(dx)
			cellY := centerY + int64(dy)
			cells = append(cells, cellX*1000000+cellY)
		}
	}
	return cells
}

func (cdm *ConsumerDeviceMatcher) loadIdleDevices() error {
	cdm.logger.Println("Loading idle devices...")

	file, err := os.Open(cdm.idleDevicesPath)
	if err != nil {
		return fmt.Errorf("failed to open idle devices file: %w", err)
	}
	defer file.Close()

	// 🆕 MODIFIED: Load from new structure with event_date field
	var idleData struct {
		EventDate        string `json:"event_date"`
		TotalIdleDevices int    `json:"total_idle_devices"`
		IdleDevices      []struct {
			DeviceID    string `json:"device_id"`
			VisitedTime string `json:"visited_time"`
			Address     string `json:"address"`
			Campaign    string `json:"campaign"`
			CampaignID  string `json:"campaign_id"`
			POIID       string `json:"poi_id"`
			Geometry    string `json:"geometry"`
		} `json:"idle_devices"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&idleData); err != nil {
		return fmt.Errorf("failed to decode idle devices: %w", err)
	}

	totalDevices := 0
	for _, device := range idleData.IdleDevices {
		lat, lon := cdm.parsePoint(device.Geometry)

		idleDevice := IdleDevice{
			DeviceID:    device.DeviceID,
			VisitedTime: device.VisitedTime,
			Address:     device.Address,
			Campaign:    device.Campaign,
			CampaignID:  device.CampaignID,
			POIID:       device.POIID,
			Latitude:    lat,
			Longitude:   lon,
		}

		cellKey := getCellKey(lat, lon)
		cdm.deviceSpatialIndex[cellKey] = append(cdm.deviceSpatialIndex[cellKey], idleDevice)
		totalDevices++
	}

	cdm.logger.Printf("✅ Loaded %d idle devices in %d grid cells", totalDevices, len(cdm.deviceSpatialIndex))
	return nil
}

func (cdm *ConsumerDeviceMatcher) parsePoint(geometryStr string) (float64, float64) {
	geometryStr = strings.TrimPrefix(geometryStr, "POINT (")
	geometryStr = strings.TrimSuffix(geometryStr, ")")

	var lon, lat float64
	fmt.Sscanf(geometryStr, "%f %f", &lon, &lat)
	return lat, lon
}

func (cdm *ConsumerDeviceMatcher) findNearbyDevices(consumerLat, consumerLon float64) []IdleDevice {
	consumerPoint := orb.Point{consumerLon, consumerLat}
	cells := getNearbyCells(consumerLat, consumerLon, MatchRadiusMeters)

	nearbyDevices := make([]IdleDevice, 0)

	cdm.indexMutex.RLock()
	defer cdm.indexMutex.RUnlock()

	for _, cellKey := range cells {
		if devices, exists := cdm.deviceSpatialIndex[cellKey]; exists {
			for _, device := range devices {
				devicePoint := orb.Point{device.Longitude, device.Latitude}
				distance := geo.Distance(consumerPoint, devicePoint)

				if distance <= MatchRadiusMeters {
					nearbyDevices = append(nearbyDevices, device)
				}
			}
		}
	}

	return nearbyDevices
}

func (cdm *ConsumerDeviceMatcher) processConsumerFiles() error {
	cdm.logger.Println("Finding consumer parquet files...")

	patterns := []string{
		filepath.Join(cdm.consumerFolder, "consumers_*.parquet"),          // 🆕 NEW: date-based files
		filepath.Join(cdm.consumerFolder, "consumer_raw_batch_*.parquet"), // OLD: fallback
		filepath.Join(cdm.consumerFolder, "consumers_chunk_*.parquet"),    // OLD: fallback
		filepath.Join(cdm.consumerFolder, "*.parquet"),
	}

	allFiles := make(map[string]bool)
	for _, pattern := range patterns {
		files, err := filepath.Glob(pattern)
		if err == nil {
			for _, f := range files {
				allFiles[f] = true
			}
		}
	}

	files := make([]string, 0, len(allFiles))
	for f := range allFiles {
		files = append(files, f)
	}

	if len(files) == 0 {
		return fmt.Errorf("no consumer parquet files found in %s", cdm.consumerFolder)
	}

	totalFiles := len(files)
	cdm.logger.Printf("Found %d consumer parquet files", totalFiles)
	cdm.logger.Printf("Using %d parallel workers", ParallelWorkers)

	startTime := time.Now()

	fileChan := make(chan string, len(files))
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	var wg sync.WaitGroup

	for w := 1; w <= ParallelWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			cdm.fileWorker(workerID, fileChan, totalFiles)
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(startTime)

	cdm.logger.Println()
	cdm.logger.Printf("✅ All files processed in %.2f seconds", elapsed.Seconds())
	cdm.logger.Printf("   Total matches found: %d", cdm.totalMatches.Load())
	cdm.logger.Printf("   Valid consumers: %d", cdm.validConsumers.Load())
	cdm.logger.Printf("   Invalid (0,0) consumers skipped: %d", cdm.invalidConsumers.Load())

	return nil
}

func (cdm *ConsumerDeviceMatcher) fileWorker(workerID int, fileChan <-chan string, totalFiles int) {
	for fpath := range fileChan {
		matches, valid, invalid, err := cdm.processConsumerFile(fpath)
		if err != nil {
			cdm.logger.Printf("[W%d] Error processing %s: %v", workerID, filepath.Base(fpath), err)
			continue
		}

		cdm.matchesMutex.Lock()
		cdm.matches = append(cdm.matches, matches...)
		cdm.matchesMutex.Unlock()

		cdm.totalMatches.Add(int64(len(matches)))
		cdm.validConsumers.Add(int64(valid))
		cdm.invalidConsumers.Add(int64(invalid))

		processed := cdm.processedFiles.Add(1)
		cdm.logger.Printf("[W%d] %s: %d matches (%d valid, %d invalid) | Total: %d/%d files, %d matches",
			workerID, filepath.Base(fpath), len(matches), valid, invalid, processed, totalFiles, cdm.totalMatches.Load())
	}
}

func (cdm *ConsumerDeviceMatcher) processConsumerFile(fpath string) ([]ConsumerDeviceMatch, int, int, error) {
	consumers, err := cdm.readConsumersFromParquetArrow(fpath)
	if err != nil {
		return nil, 0, 0, err
	}

	matches := make([]ConsumerDeviceMatch, 0)
	validCount := 0
	invalidCount := 0

	for _, consumer := range consumers {
		// Skip invalid (0,0) coordinates
		if consumer.Latitude == 0 && consumer.Longitude == 0 {
			invalidCount++
			continue
		}

		validCount++

		nearbyDevices := cdm.findNearbyDevices(consumer.Latitude, consumer.Longitude)

		for _, device := range nearbyDevices {
			cdm.matchesMutex.Lock()
			alreadyMatched := cdm.matchedDevices[device.DeviceID]
			if !alreadyMatched {
				cdm.matchedDevices[device.DeviceID] = true
			}
			cdm.matchesMutex.Unlock()

			if alreadyMatched {
				continue
			}

			datePart, timePart := cdm.splitDateTime(device.VisitedTime)

			match := ConsumerDeviceMatch{
				DeviceID:    device.DeviceID,
				DateVisited: datePart,
				TimeVisited: timePart,
				ConsumerID:  consumer.ID,
				Name:        cdm.buildName(consumer.PersonFirstName, consumer.PersonLastName),
				Address:     consumer.PrimaryAddress,
				Email:       consumer.Email,
				CityName:    consumer.CityName,
				State:       consumer.State,
				ZipCode:     consumer.ZipCode,
				POI:         device.Address,
				Campaign:    device.Campaign,
				CampaignID:  device.CampaignID,
				POIID:       device.POIID,
				Phone:       consumer.TenDigitPhone,
			}

			matches = append(matches, match)
			break
		}
	}

	return matches, validCount, invalidCount, nil
}

func (cdm *ConsumerDeviceMatcher) readConsumersFromParquetArrow(fpath string) ([]ConsumerRecord, error) {
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

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 10000}, memory.NewGoAllocator())
	if err != nil {
		return nil, err
	}

	recordReader, err := arrowReader.GetRecordReader(context.TODO(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer recordReader.Release()

	consumers := make([]ConsumerRecord, 0)

	for recordReader.Next() {
		rec := recordReader.Record()
		rows := int(rec.NumRows())
		fields := rec.Schema().Fields()

		colIndex := make(map[string]int)
		for i, f := range fields {
			colIndex[f.Name] = i
		}

		for i := 0; i < rows; i++ {
			consumer := ConsumerRecord{}

			if idx, ok := colIndex["id"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Uint64:
					consumer.ID = col.Value(i)
				case *array.Int64:
					consumer.ID = uint64(col.Value(i))
				case *array.String:
					fmt.Sscanf(col.Value(i), "%d", &consumer.ID)
				}
			}

			if idx, ok := colIndex["latitude"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Float64:
					consumer.Latitude = col.Value(i)
				case *array.Float32:
					consumer.Latitude = float64(col.Value(i))
				}
			}

			if idx, ok := colIndex["longitude"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Float64:
					consumer.Longitude = col.Value(i)
				case *array.Float32:
					consumer.Longitude = float64(col.Value(i))
				}
			}

			if consumer.Latitude < -90 || consumer.Latitude > 90 ||
				consumer.Longitude < -180 || consumer.Longitude > 180 {
				continue
			}

			if idx, ok := colIndex["PersonFirstName"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.PersonFirstName = col.Value(i)
				}
			}
			if idx, ok := colIndex["PersonLastName"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.PersonLastName = col.Value(i)
				}
			}
			if idx, ok := colIndex["PrimaryAddress"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.PrimaryAddress = col.Value(i)
				}
			}
			if idx, ok := colIndex["TenDigitPhone"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.TenDigitPhone = col.Value(i)
				}
			}
			if idx, ok := colIndex["Email"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.Email = col.Value(i)
				}
			}
			if idx, ok := colIndex["CityName"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.CityName = col.Value(i)
				}
			}
			if idx, ok := colIndex["State"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.State = col.Value(i)
				}
			}
			if idx, ok := colIndex["ZipCode"]; ok {
				if col, ok := rec.Column(idx).(*array.String); ok && !col.IsNull(i) {
					consumer.ZipCode = col.Value(i)
				}
			}

			consumers = append(consumers, consumer)
		}
		rec.Release()
	}

	return consumers, nil
}

func (cdm *ConsumerDeviceMatcher) saveMatches(processedDate string, processingTimeMs int64) error {
	cdm.logger.Println("\nSaving matches...")

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

func (cdm *ConsumerDeviceMatcher) splitDateTime(visitedTime string) (string, string) {
	parts := strings.Split(visitedTime, " ")
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if strings.Contains(visitedTime, "T") {
		parts = strings.Split(visitedTime, "T")
		if len(parts) == 2 {
			return parts[0], strings.TrimSuffix(parts[1], "Z")
		}
	}
	return visitedTime, ""
}

func (cdm *ConsumerDeviceMatcher) buildName(firstName, lastName string) string {
	if firstName == "" && lastName == "" {
		return ""
	}
	return strings.TrimSpace(firstName + " " + lastName)
}

func (cdm *ConsumerDeviceMatcher) Run() error {
	cdm.logger.Println("╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  STEP 4: CONSUMER-DEVICE MATCHING                    ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

	startTime := time.Now()

	if err := cdm.loadIdleDevices(); err != nil {
		return fmt.Errorf("failed to load idle devices: %w", err)
	}

	if err := cdm.processConsumerFiles(); err != nil {
		return fmt.Errorf("failed to process consumer files: %w", err)
	}

	elapsed := time.Since(startTime)

	if err := cdm.saveMatches(cdm.processedDate, elapsed.Milliseconds()); err != nil {
		return fmt.Errorf("failed to save matches: %w", err)
	}

	cdm.logger.Printf("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Printf("║  ✅ STEP 4 COMPLETED IN %.2f seconds", elapsed.Seconds())
	cdm.logger.Printf("╚═══════════════════════════════════════════════════════╝")

	runtime.GC()
	return nil
}
