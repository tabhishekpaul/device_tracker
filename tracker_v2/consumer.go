package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	ParallelWorkers   = 1 // Single worker for debugging
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
	CampaignID  string
	POIID       string
	Latitude    float64
	Longitude   float64
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
}

func NewConsumerDeviceMatcher(outputFolder, consumerFolder, idleDevicesPath string) *ConsumerDeviceMatcher {
	return &ConsumerDeviceMatcher{
		outputFolder:       outputFolder,
		consumerFolder:     consumerFolder,
		idleDevicesPath:    idleDevicesPath,
		logger:             log.New(os.Stdout, "[DEBUG] ", log.LstdFlags),
		deviceSpatialIndex: make(map[int64][]IdleDevice),
		matches:            make([]ConsumerDeviceMatch, 0),
		matchedDevices:     make(map[string]bool),
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
	cdm.logger.Println("╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  LOADING IDLE DEVICES - DEBUG MODE                   ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

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

	totalDevices := 0
	sampleCount := 0

	for date, devices := range idleData.IdleDevicesByDate {
		cdm.logger.Printf("\n📅 Date: %s (%d devices)", date, len(devices))

		for _, device := range devices {
			lat, lon := cdm.parsePoint(device.Geometry)

			// Show first 3 devices as samples
			if sampleCount < 3 {
				cdm.logger.Printf("  Sample Device #%d:", sampleCount+1)
				cdm.logger.Printf("    DeviceID:    %s", device.DeviceID)
				cdm.logger.Printf("    VisitedTime: %s", device.VisitedTime)
				cdm.logger.Printf("    Address:     %s", device.Address)
				cdm.logger.Printf("    Campaign:    %s", device.Campaign)
				cdm.logger.Printf("    Lat/Lon:     %.6f, %.6f", lat, lon)
				cdm.logger.Printf("    Grid Cell:   %d", getCellKey(lat, lon))
				sampleCount++
			}

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
	}

	cdm.logger.Printf("\n✅ Total idle devices loaded: %d", totalDevices)
	cdm.logger.Printf("✅ Spatial index cells: %d", len(cdm.deviceSpatialIndex))

	// Show grid cell distribution
	cdm.logger.Println("\n📊 Grid Cell Distribution (top 10):")
	type cellInfo struct {
		key   int64
		count int
	}
	cells := make([]cellInfo, 0, len(cdm.deviceSpatialIndex))
	for k, v := range cdm.deviceSpatialIndex {
		cells = append(cells, cellInfo{k, len(v)})
	}
	// Sort by count
	for i := 0; i < len(cells)-1; i++ {
		for j := i + 1; j < len(cells); j++ {
			if cells[j].count > cells[i].count {
				cells[i], cells[j] = cells[j], cells[i]
			}
		}
	}
	for i := 0; i < 10 && i < len(cells); i++ {
		cdm.logger.Printf("  Cell %d: %d devices", cells[i].key, cells[i].count)
	}

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
	cdm.logger.Println("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  PROCESSING CONSUMER FILES - DEBUG MODE               ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

	patterns := []string{
		filepath.Join(cdm.consumerFolder, "consumers_chunk_*.parquet"),
		filepath.Join(cdm.consumerFolder, "consumer_raw_batch_*.parquet"),
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

	cdm.logger.Printf("\nFound %d consumer parquet files", len(files))
	cdm.logger.Println("Processing FIRST FILE ONLY for debugging...")

	// Process only first file for debugging
	if len(files) > 0 {
		fpath := files[0]
		matches, err := cdm.processConsumerFileDebug(fpath)
		if err != nil {
			return err
		}

		cdm.logger.Printf("\n✅ File processed: %d matches found", len(matches))
		cdm.matches = append(cdm.matches, matches...)
	}

	return nil
}

func (cdm *ConsumerDeviceMatcher) processConsumerFileDebug(fpath string) ([]ConsumerDeviceMatch, error) {
	cdm.logger.Printf("📂 Processing: %s\n", filepath.Base(fpath))

	consumers, err := cdm.readConsumersFromParquetArrow(fpath)
	if err != nil {
		return nil, err
	}

	cdm.logger.Printf("✅ Loaded %d consumers from file\n", len(consumers))

	if len(consumers) == 0 {
		cdm.logger.Println("⚠️  No consumers found in file!")
		return nil, nil
	}

	// Show first 5 consumers
	cdm.logger.Println("📋 Sample Consumers:")
	for i := 0; i < 5 && i < len(consumers); i++ {
		c := consumers[i]
		cdm.logger.Printf("  Consumer #%d:", i+1)
		cdm.logger.Printf("    ID:       %d", c.ID)
		cdm.logger.Printf("    Name:     %s %s", c.PersonFirstName, c.PersonLastName)
		cdm.logger.Printf("    Address:  %s", c.PrimaryAddress)
		cdm.logger.Printf("    Lat/Lon:  %.6f, %.6f", c.Latitude, c.Longitude)
		cdm.logger.Printf("    Grid Cell: %d", getCellKey(c.Latitude, c.Longitude))

		// Check for nearby devices
		nearby := cdm.findNearbyDevices(c.Latitude, c.Longitude)
		cdm.logger.Printf("    Nearby devices: %d", len(nearby))
		if len(nearby) > 0 {
			cdm.logger.Printf("    ✅ MATCH POSSIBLE!")
			for j, d := range nearby {
				if j < 2 {
					devicePoint := orb.Point{d.Longitude, d.Latitude}
					consumerPoint := orb.Point{c.Longitude, c.Latitude}
					distance := geo.Distance(consumerPoint, devicePoint)
					cdm.logger.Printf("      Device: %s (distance: %.2fm)", d.DeviceID, distance)
				}
			}
		}
	}

	matches := make([]ConsumerDeviceMatch, 0)
	matchCount := 0

	cdm.logger.Println("\n🔍 Searching for matches...")

	for idx, consumer := range consumers {
		if idx%10000 == 0 && idx > 0 {
			cdm.logger.Printf("  Processed %d consumers, %d matches so far...", idx, matchCount)
		}

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
			}

			matches = append(matches, match)
			matchCount++

			if matchCount <= 5 {
				cdm.logger.Printf("\n🎯 MATCH FOUND #%d:", matchCount)
				cdm.logger.Printf("  Device: %s at %.6f, %.6f", device.DeviceID, device.Latitude, device.Longitude)
				cdm.logger.Printf("  Consumer: %d (%s) at %.6f, %.6f", consumer.ID, match.Name, consumer.Latitude, consumer.Longitude)
				devicePoint := orb.Point{device.Longitude, device.Latitude}
				consumerPoint := orb.Point{consumer.Longitude, consumer.Latitude}
				distance := geo.Distance(consumerPoint, devicePoint)
				cdm.logger.Printf("  Distance: %.2f meters", distance)
			}

			break
		}
	}

	return matches, nil
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

	ctx := context.Background()
	recordReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
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

			if idx, ok := colIndex["Latitude"]; ok {
				switch col := rec.Column(idx).(type) {
				case *array.Float64:
					consumer.Latitude = col.Value(i)
				case *array.Float32:
					consumer.Latitude = float64(col.Value(i))
				}
			}

			if idx, ok := colIndex["Longitude"]; ok {
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

func (cdm *ConsumerDeviceMatcher) Run() error {
	startTime := time.Now()

	if err := cdm.loadIdleDevices(); err != nil {
		return fmt.Errorf("failed to load idle devices: %w", err)
	}

	if err := cdm.processConsumerFiles(); err != nil {
		return fmt.Errorf("failed to process consumer files: %w", err)
	}

	elapsed := time.Since(startTime)

	cdm.logger.Printf("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Printf("║  DEBUG SUMMARY")
	cdm.logger.Printf("╚═══════════════════════════════════════════════════════╝")
	cdm.logger.Printf("Total matches found: %d", len(cdm.matches))
	cdm.logger.Printf("Time elapsed: %.2f seconds", elapsed.Seconds())

	return nil
}
