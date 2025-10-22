package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

// ============================================================================
// STEP 3: MEMORY-EFFICIENT IDLE DEVICE DETECTION (520M DEVICES)
// - Process one parquet file at a time
// - Release memory after each file
// - Save to JSON grouped by event_timestamp date
// - Final merge into single Idle_devices.json
// ============================================================================

// CampaignMetadata holds the campaign info for a device
type CampaignMetadata struct {
	DeviceID       string    `json:"device_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Address        string    `json:"address"`
	Campaign       string    `json:"campaign"`
	CampaignID     string    `json:"campaign_id"`
	POIID          string    `json:"poi_id"`
}

// IdleDeviceResult represents the final idle device output
type IdleDeviceResult struct {
	DeviceID    string `json:"device_id"`
	VisitedTime string `json:"visited_time"`
	Address     string `json:"address"`
	Campaign    string `json:"campaign"`
	CampaignID  string `json:"campaign_id"`
	POIID       string `json:"poi_id"`
	Geometry    string `json:"geometry"`
}

// IdleDevicesByDate groups idle devices by event date
type IdleDevicesByDate struct {
	EventDate        string             `json:"event_date"`
	TotalIdleDevices int                `json:"total_idle_devices"`
	IdleDevices      []IdleDeviceResult `json:"idle_devices"`
}

// FinalIdleDevicesOutput is the final merged JSON output
type FinalIdleDevicesOutput struct {
	ProcessedDates    []string                      `json:"processed_dates"`
	TotalIdleDevices  int                           `json:"total_idle_devices"`
	ProcessingTimeMs  int64                         `json:"processing_time_ms"`
	IdleDevicesByDate map[string][]IdleDeviceResult `json:"idle_devices_by_date"`
}

// ============================================================================
// MAIN ORCHESTRATION METHOD
// ============================================================================

// RunIdleDeviceSearch orchestrates the idle device search for multiple dates
func (dt *DeviceTracker) RunIdleDeviceSearch(folderList []string, targetDates []string) error {
	fmt.Println("\n╔════════════════════════════════════════════════════════╗")
	fmt.Println("║   STEP 3: MEMORY-EFFICIENT IDLE DEVICE DETECTION      ║")
	fmt.Println("╚════════════════════════════════════════════════════════╝")
	overallStartTime := time.Now()

	// Create output directory
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for _, targetDate := range targetDates {
		fmt.Printf("\n🔍 Processing Date: %s\n", targetDate)
		err := dt.FindIdleDevicesStreamingByDate(folderList, targetDate)
		if err != nil {
			fmt.Printf("  ⚠️  Error processing %s: %v\n", targetDate, err)
			continue
		}

		// Force garbage collection after each date
		runtime.GC()
	}

	// Merge all event date files into final output
	err := dt.MergeIdleDevicesByEventDate()
	if err != nil {
		return fmt.Errorf("failed to merge idle devices: %w", err)
	}

	duration := time.Since(overallStartTime)
	fmt.Printf("\n✅ Step 3 Completed in %v\n", duration)
	return nil
}

// ============================================================================
// STREAMING PARQUET PROCESSING (ONE FILE AT A TIME)
// ============================================================================

// FindIdleDevicesStreamingByDate processes parquet files one-by-one and groups by event_timestamp
func (dt *DeviceTracker) FindIdleDevicesStreamingByDate(folderList []string, targetDate string) error {
	startTime := time.Now()

	// Load campaign metadata once
	campaignMetadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return fmt.Errorf("failed to load campaign metadata: %w", err)
	}
	fmt.Printf("  📋 Loaded metadata for %d devices\n", len(campaignMetadata))

	// Map to accumulate idle devices by event date
	idleDevicesByEventDate := make(map[string][]IdleDeviceResult)
	var mu sync.Mutex

	totalProcessed := 0
	totalIdle := 0

	// Process each parquet file sequentially (one at a time)
	for _, folderName := range folderList {
		parquetPath := filepath.Join(
			dt.OutputFolder,
			"time_filtered",
			strings.TrimPrefix(folderName, "load_date="),
			fmt.Sprintf("time_filtered_loaddate%s.parquet", strings.TrimPrefix(folderName, "load_date=")),
		)

		if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
			fmt.Printf("    ⚠️  File not found: %s\n", parquetPath)
			continue
		}

		fmt.Printf("    📂 Processing: %s\n", filepath.Base(parquetPath))

		// Process this single parquet file
		idleDevices, processed, err := dt.processParquetFileForIdleDevices(parquetPath, campaignMetadata)
		if err != nil {
			fmt.Printf("    ⚠️  Error: %v\n", err)
			continue
		}

		totalProcessed += processed
		totalIdle += len(idleDevices)

		// Group idle devices by event date
		mu.Lock()
		for _, idleDevice := range idleDevices {
			// Extract date from visited_time (format: 2025-10-21T14:30:00Z)
			eventDate := idleDevice.VisitedTime[:10] // "2025-10-21"
			idleDevicesByEventDate[eventDate] = append(idleDevicesByEventDate[eventDate], idleDevice)
		}
		mu.Unlock()

		// Force GC after processing each file
		runtime.GC()

		fmt.Printf("    ✓ Processed: %d records, Found: %d idle devices\n", processed, len(idleDevices))
	}

	// Save idle devices grouped by event date
	err = dt.saveIdleDevicesByEventDate(idleDevicesByEventDate)
	if err != nil {
		return fmt.Errorf("failed to save idle devices: %w", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("  ✅ Total: %d records processed, %d idle devices found in %v\n", totalProcessed, totalIdle, duration)

	return nil
}

// processParquetFileForIdleDevices processes a single parquet file and returns idle devices
func (dt *DeviceTracker) processParquetFileForIdleDevices(
	parquetPath string,
	campaignMetadata map[string]CampaignMetadata,
) ([]IdleDeviceResult, int, error) {

	// Read parquet file
	records, err := dt.readTimeFilteredParquet(parquetPath)
	if err != nil {
		return nil, 0, err
	}

	if len(records) == 0 {
		return nil, 0, nil
	}

	totalRecords := len(records)

	// Group by device_id
	deviceGroups := dt.groupByDeviceIDOptimized(records)

	// Release original records memory
	records = nil
	runtime.GC()

	// Process device groups to find idle devices
	idleDevices := dt.processDeviceGroupsParallel(deviceGroups, campaignMetadata)

	// Release device groups memory
	deviceGroups = nil
	runtime.GC()

	return idleDevices, totalRecords, nil
}

// ============================================================================
// SAVE IDLE DEVICES BY EVENT DATE
// ============================================================================

// saveIdleDevicesByEventDate saves idle devices to separate JSON files per event date
func (dt *DeviceTracker) saveIdleDevicesByEventDate(idleDevicesByEventDate map[string][]IdleDeviceResult) error {
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for eventDate, devices := range idleDevicesByEventDate {
		// Format: idle_devices_2025-10-21.json
		jsonPath := filepath.Join(idleDevicesFolder, fmt.Sprintf("idle_devices_%s.json", eventDate))

		// Load existing data if file exists
		existingDevices := make([]IdleDeviceResult, 0)
		if _, err := os.Stat(jsonPath); err == nil {
			existingData, err := dt.loadIdleDevicesFromJSON(jsonPath)
			if err == nil {
				existingDevices = existingData
			}
		}

		// Merge with new devices
		allDevices := append(existingDevices, devices...)

		// Remove duplicates by device_id
		uniqueDevices := dt.deduplicateIdleDevices(allDevices)

		// Create output structure
		output := IdleDevicesByDate{
			EventDate:        eventDate,
			TotalIdleDevices: len(uniqueDevices),
			IdleDevices:      uniqueDevices,
		}

		// Write to JSON
		file, err := os.Create(jsonPath)
		if err != nil {
			return fmt.Errorf("failed to create JSON file %s: %w", jsonPath, err)
		}

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			file.Close()
			return fmt.Errorf("failed to encode JSON: %w", err)
		}
		file.Close()

		fmt.Printf("    💾 Saved %d idle devices to: %s\n", len(uniqueDevices), filepath.Base(jsonPath))
	}

	return nil
}

// loadIdleDevicesFromJSON loads idle devices from a JSON file
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

// deduplicateIdleDevices removes duplicate devices (keep first occurrence)
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

// ============================================================================
// MERGE ALL EVENT DATE FILES INTO FINAL OUTPUT
// ============================================================================

// MergeIdleDevicesByEventDate merges all idle_devices_*.json into Idle_devices.json
func (dt *DeviceTracker) MergeIdleDevicesByEventDate() error {
	fmt.Println("\n📦 Merging idle device files by event date...")
	startTime := time.Now()

	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	pattern := filepath.Join(idleDevicesFolder, "idle_devices_*.json")

	// Find all event date files
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to find idle device files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no idle device files found in %s", idleDevicesFolder)
	}

	fmt.Printf("  📂 Found %d event date files to merge\n", len(files))

	// Accumulate all idle devices by date
	allIdleDevicesByDate := make(map[string][]IdleDeviceResult)
	processedDates := make([]string, 0)
	totalDevices := 0

	for _, filePath := range files {
		// Extract event date from filename: idle_devices_2025-10-21.json
		fileName := filepath.Base(filePath)
		eventDate := strings.TrimPrefix(fileName, "idle_devices_")
		eventDate = strings.TrimSuffix(eventDate, ".json")

		// Load devices from file
		devices, err := dt.loadIdleDevicesFromJSON(filePath)
		if err != nil {
			fmt.Printf("  ⚠️  Failed to load %s: %v\n", fileName, err)
			continue
		}

		allIdleDevicesByDate[eventDate] = devices
		processedDates = append(processedDates, eventDate)
		totalDevices += len(devices)

		fmt.Printf("  ✓ Loaded %d devices from %s\n", len(devices), fileName)
	}

	// Sort processed dates
	sort.Strings(processedDates)

	// Create final output
	finalOutput := FinalIdleDevicesOutput{
		ProcessedDates:    processedDates,
		TotalIdleDevices:  totalDevices,
		ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		IdleDevicesByDate: allIdleDevicesByDate,
	}

	// Write final merged JSON
	finalJSONPath := filepath.Join(dt.OutputFolder, "Idle_devices.json")
	file, err := os.Create(finalJSONPath)
	if err != nil {
		return fmt.Errorf("failed to create final JSON: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(finalOutput); err != nil {
		return fmt.Errorf("failed to encode final JSON: %w", err)
	}

	fmt.Printf("  💾 Final merged output: %s\n", finalJSONPath)
	fmt.Printf("  📊 Total idle devices: %d across %d dates\n", totalDevices, len(processedDates))
	fmt.Printf("  ⏱️  Merge completed in %v\n", time.Since(startTime))

	return nil
}

// ============================================================================
// JSON METADATA LOADING (FROM STEP 1 OUTPUT)
// ============================================================================

// GetUniqIdDataFrame loads unique device metadata from JSON file for specific target date
func (dt *DeviceTracker) GetUniqIdDataFrame() (map[string]CampaignMetadata, error) {
	// Get yesterday's date (the target date we're processing)
	yesterday := time.Now().AddDate(0, 0, -1)
	dateStr := yesterday.Format("20060102") // Format: YYYYMMDD (e.g., 20251021)

	// Build JSON file path: campaign_intersection/20251021/campaign_devices_20251021.json
	jsonPath := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr, fmt.Sprintf("campaign_devices_%s.json", dateStr))

	fmt.Printf("  📄 Reading campaign metadata from: %s\n", jsonPath)

	// Check if file exists
	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("campaign intersection JSON not found: %s (make sure Step 1 ran for this date)", jsonPath)
	}

	// Load and parse JSON
	return dt.loadMetadataFromJSON(jsonPath)
}

// loadMetadataFromJSON reads the CampaignIntersectionOutput JSON and extracts device metadata
func (dt *DeviceTracker) loadMetadataFromJSON(jsonPath string) (map[string]CampaignMetadata, error) {
	startTime := time.Now()

	// Open JSON file
	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}
	defer file.Close()

	// Parse JSON into CampaignIntersectionOutput structure
	var campaignOutput CampaignIntersectionOutput
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&campaignOutput); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	// Extract metadata from nested structure: Campaigns -> POIs -> Devices
	metadata := make(map[string]CampaignMetadata, 100000)

	// Iterate through all campaigns
	for _, campaign := range campaignOutput.Campaigns {
		// Iterate through all POIs in this campaign
		for _, poi := range campaign.POIs {
			// Iterate through all devices in this POI
			for _, device := range poi.Devices {
				deviceID := device.DeviceID

				// Only keep first occurrence of each device (deduplication)
				if _, exists := metadata[deviceID]; exists {
					continue
				}

				// Create metadata entry with campaign, POI, and device info
				metadata[deviceID] = CampaignMetadata{
					DeviceID:       deviceID,
					EventTimestamp: device.EventTimestamp,
					Address:        poi.POIName, // POI name as address
					Campaign:       campaign.CampaignName,
					CampaignID:     campaign.CampaignID,
					POIID:          poi.POIID,
				}
			}
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("  ✅ Loaded metadata for %d unique devices from JSON in %v\n", len(metadata), duration)

	// Validate we got data
	if len(metadata) == 0 {
		return nil, fmt.Errorf("no device metadata found in JSON file - JSON might be empty or malformed")
	}

	return metadata, nil
}

// ============================================================================
// PARQUET READING (MEMORY EFFICIENT)
// ============================================================================

// readTimeFilteredParquet reads parquet file efficiently
func (dt *DeviceTracker) readTimeFilteredParquet(filePath string) ([]DeviceRecord, error) {
	pf, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()
	if numRowGroups == 0 {
		return nil, nil
	}

	records := make([]DeviceRecord, 0)
	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		rgRecords, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}
		records = append(records, rgRecords...)
	}

	return records, nil
}

// ============================================================================
// DEVICE GROUPING AND PROCESSING
// ============================================================================

// groupByDeviceIDOptimized groups records with pre-allocated map
func (dt *DeviceTracker) groupByDeviceIDOptimized(records []DeviceRecord) map[string][]DeviceRecord {
	// Pre-allocate map with estimated size
	groups := make(map[string][]DeviceRecord, len(records)/10)

	for i := range records {
		deviceID := records[i].DeviceID
		groups[deviceID] = append(groups[deviceID], records[i])
	}

	// Sort each group by timestamp
	for deviceID := range groups {
		recs := groups[deviceID]
		sort.Slice(recs, func(i, j int) bool {
			return recs[i].EventTimestamp.Before(recs[j].EventTimestamp)
		})
	}

	return groups
}

// processDeviceGroupsParallel processes device groups using worker pool
func (dt *DeviceTracker) processDeviceGroupsParallel(
	deviceGroups map[string][]DeviceRecord,
	campaignMetadata map[string]CampaignMetadata,
) []IdleDeviceResult {

	numWorkers := dt.NumWorkers
	if numWorkers == 0 {
		numWorkers = 8
	}

	// Create job channel
	jobs := make(chan string, len(deviceGroups))
	results := make(chan IdleDeviceResult, len(deviceGroups))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for deviceID := range jobs {
				if result, isIdle := dt.checkIfIdle(deviceID, deviceGroups[deviceID], campaignMetadata); isIdle {
					results <- result
				}
			}
		}()
	}

	// Send jobs
	for deviceID := range deviceGroups {
		jobs <- deviceID
	}
	close(jobs)

	// Wait and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	idleDevices := make([]IdleDeviceResult, 0)
	for result := range results {
		idleDevices = append(idleDevices, result)
	}

	return idleDevices
}

// checkIfIdle determines if a device is idle within buffer
func (dt *DeviceTracker) checkIfIdle(
	deviceID string,
	records []DeviceRecord,
	campaignMetadata map[string]CampaignMetadata,
) (IdleDeviceResult, bool) {

	if len(records) == 0 {
		return IdleDeviceResult{}, false
	}

	// Get campaign metadata
	metadata, exists := campaignMetadata[deviceID]
	if !exists {
		return IdleDeviceResult{}, false
	}

	// First point
	firstRecord := records[0]
	firstPoint := orb.Point{firstRecord.Longitude, firstRecord.Latitude}

	// Check if all points within buffer
	bufferMeters := dt.IdleDeviceBuffer

	for i := range records {
		point := orb.Point{records[i].Longitude, records[i].Latitude}
		distance := geo.Distance(firstPoint, point)

		if distance > bufferMeters {
			return IdleDeviceResult{}, false
		}
	}

	// All points within buffer - idle device found
	return IdleDeviceResult{
		DeviceID:    deviceID,
		VisitedTime: metadata.EventTimestamp.Format(time.RFC3339),
		Address:     metadata.Address,
		Campaign:    metadata.Campaign,
		CampaignID:  metadata.CampaignID,
		POIID:       metadata.POIID,
		Geometry:    fmt.Sprintf("POINT (%f %f)", firstRecord.Longitude, firstRecord.Latitude),
	}, true
}

// ============================================================================
// UTILITY METHOD
// ============================================================================

// GetUniqueIdList returns list of unique device IDs
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
