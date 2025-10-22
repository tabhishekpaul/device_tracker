package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

// ============================================================================
// STEP 3: STREAMING ROW-GROUP PROCESSING (FIXED IDLE DETECTION)
// - Process parquet ROW GROUPS one at a time
// - Keep device states across ALL row groups
// - Save idle devices only at the END
// - Never load entire parquet into memory
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

// DeviceTrackingState tracks a device's position history for idle detection
type DeviceTrackingState struct {
	FirstPoint  orb.Point
	FirstTime   time.Time
	IsStillIdle bool // Still within buffer
	RecordCount int
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
	fmt.Println("║   STEP 3: STREAMING IDLE DEVICE DETECTION            ║")
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
// STREAMING ROW-GROUP PROCESSING (FIXED LOGIC)
// ============================================================================

// FindIdleDevicesStreamingByDate processes parquet row groups one-by-one
func (dt *DeviceTracker) FindIdleDevicesStreamingByDate(folderList []string, targetDate string) error {
	startTime := time.Now()

	// Load campaign metadata once
	campaignMetadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return fmt.Errorf("failed to load campaign metadata: %w", err)
	}
	fmt.Printf("  📋 Loaded metadata for %d devices\n", len(campaignMetadata))

	totalProcessed := 0
	totalIdle := 0

	// Process each parquet file
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

		// STREAMING: Process row groups one at a time
		idleCount, recordCount, err := dt.processParquetFileStreaming(parquetPath, campaignMetadata)
		if err != nil {
			fmt.Printf("    ⚠️  Error: %v\n", err)
			continue
		}

		totalProcessed += recordCount
		totalIdle += idleCount

		fmt.Printf("    ✓ Processed: %d records, Found: %d idle devices\n", recordCount, idleCount)

		// Force GC after each file
		runtime.GC()
	}

	duration := time.Since(startTime)
	fmt.Printf("  ✅ Total: %d records processed, %d idle devices found in %v\n", totalProcessed, totalIdle, duration)

	return nil
}

// processParquetFileStreaming - Process row groups one at a time, keep states until end
func (dt *DeviceTracker) processParquetFileStreaming(
	parquetPath string,
	campaignMetadata map[string]CampaignMetadata,
) (int, int, error) {

	// Open parquet file
	pf, err := file.OpenParquetFile(parquetPath, false)
	if err != nil {
		return 0, 0, err
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()
	if numRowGroups == 0 {
		return 0, 0, nil
	}

	fmt.Printf("      📊 File has %d row groups\n", numRowGroups)

	// Device tracking state persists across ALL row groups (FIX: Don't clear until end)
	deviceStates := make(map[string]*DeviceTrackingState)
	totalRecords := 0

	// Process each row group sequentially
	for rgIdx := 0; rgIdx < numRowGroups; rgIdx++ {
		if rgIdx%50 == 0 || rgIdx == numRowGroups-1 {
			fmt.Printf("      🔄 Processing row group %d/%d (tracking %d devices)...\n",
				rgIdx+1, numRowGroups, len(deviceStates))
		}

		// Read one row group
		records, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			fmt.Printf("      ⚠️  Failed to read row group %d: %v\n", rgIdx, err)
			continue
		}

		totalRecords += len(records)

		// Update device states with records from this row group
		dt.updateDeviceStates(deviceStates, records, campaignMetadata)

		// Release row group memory
		records = nil

		// Periodic GC every 50 row groups
		if rgIdx%50 == 0 {
			runtime.GC()
		}
	}

	fmt.Printf("      ✅ Finished processing all row groups. Analyzing %d tracked devices...\n", len(deviceStates))

	// NOW save idle devices (only at the end after processing all row groups)
	idleDeviceCount := dt.saveIdleDevicesFromStates(deviceStates, campaignMetadata)

	// Clear all states
	deviceStates = nil
	runtime.GC()

	return idleDeviceCount, totalRecords, nil
}

// updateDeviceStates updates tracking state for devices in this row group
func (dt *DeviceTracker) updateDeviceStates(
	deviceStates map[string]*DeviceTrackingState,
	records []DeviceRecord,
	campaignMetadata map[string]CampaignMetadata,
) {
	bufferMeters := dt.IdleDeviceBuffer

	for i := range records {
		record := &records[i]
		deviceID := record.DeviceID

		// Only track devices that are in campaign metadata
		if _, exists := campaignMetadata[deviceID]; !exists {
			continue
		}

		state, exists := deviceStates[deviceID]
		if !exists {
			// First time seeing this device
			deviceStates[deviceID] = &DeviceTrackingState{
				FirstPoint:  orb.Point{record.Longitude, record.Latitude},
				FirstTime:   record.EventTimestamp,
				IsStillIdle: true,
				RecordCount: 1,
			}
			continue
		}

		// Device already tracked - check if still idle
		if state.IsStillIdle {
			point := orb.Point{record.Longitude, record.Latitude}
			distance := geo.Distance(state.FirstPoint, point)

			if distance > bufferMeters {
				// Device moved outside buffer - not idle
				state.IsStillIdle = false
			}
		}
		state.RecordCount++
	}
}

// saveIdleDevicesFromStates - NEW: Save idle devices after processing all row groups
func (dt *DeviceTracker) saveIdleDevicesFromStates(
	deviceStates map[string]*DeviceTrackingState,
	campaignMetadata map[string]CampaignMetadata,
) int {

	idleDevicesByEventDate := make(map[string][]IdleDeviceResult)

	// Collect idle devices (devices that stayed within buffer with 2+ records)
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

			// Group by event date
			eventDate := idleDevice.VisitedTime[:10]
			idleDevicesByEventDate[eventDate] = append(idleDevicesByEventDate[eventDate], idleDevice)
		}
	}

	// Save to JSON files
	savedCount := 0
	if len(idleDevicesByEventDate) > 0 {
		dt.saveIdleDevicesToFiles(idleDevicesByEventDate)
		for _, devices := range idleDevicesByEventDate {
			savedCount += len(devices)
		}
		fmt.Printf("      💾 Saved %d idle devices to JSON files\n", savedCount)
	} else {
		fmt.Printf("      ⚠️  No idle devices found\n")
	}

	return savedCount
}

// saveIdleDevicesToFiles saves idle devices to their respective event date files
func (dt *DeviceTracker) saveIdleDevicesToFiles(idleDevicesByEventDate map[string][]IdleDeviceResult) {
	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for eventDate, newDevices := range idleDevicesByEventDate {
		jsonPath := filepath.Join(idleDevicesFolder, fmt.Sprintf("idle_devices_%s.json", eventDate))

		// Load existing devices
		existingDevices := make([]IdleDeviceResult, 0)
		if _, err := os.Stat(jsonPath); err == nil {
			existingData, err := dt.loadIdleDevicesFromJSON(jsonPath)
			if err == nil {
				existingDevices = existingData
			}
		}

		// Merge and deduplicate
		allDevices := append(existingDevices, newDevices...)
		uniqueDevices := dt.deduplicateIdleDevices(allDevices)

		// Save
		output := IdleDevicesByDate{
			EventDate:        eventDate,
			TotalIdleDevices: len(uniqueDevices),
			IdleDevices:      uniqueDevices,
		}

		file, err := os.Create(jsonPath)
		if err != nil {
			fmt.Printf("      ⚠️  Failed to create %s: %v\n", jsonPath, err)
			continue
		}

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		encoder.Encode(output)
		file.Close()

		fmt.Printf("        ✓ Saved %d devices to %s\n", len(uniqueDevices), filepath.Base(jsonPath))
	}
}

// ============================================================================
// JSON FILE OPERATIONS
// ============================================================================

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

	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to find idle device files: %w", err)
	}

	if len(files) == 0 {
		fmt.Println("  ⚠️  No idle device files found - no idle devices detected")
		return nil // Not an error, just no idle devices found
	}

	fmt.Printf("  📂 Found %d event date files to merge\n", len(files))

	allIdleDevicesByDate := make(map[string][]IdleDeviceResult)
	processedDates := make([]string, 0)
	totalDevices := 0

	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		eventDate := strings.TrimPrefix(fileName, "idle_devices_")
		eventDate = strings.TrimSuffix(eventDate, ".json")

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

	sort.Strings(processedDates)

	finalOutput := FinalIdleDevicesOutput{
		ProcessedDates:    processedDates,
		TotalIdleDevices:  totalDevices,
		ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		IdleDevicesByDate: allIdleDevicesByDate,
	}

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
	yesterday := time.Now().AddDate(0, 0, -1)
	dateStr := yesterday.Format("20060102")

	jsonPath := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr, fmt.Sprintf("campaign_devices_%s.json", dateStr))

	fmt.Printf("  📄 Reading campaign metadata from: %s\n", jsonPath)

	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("campaign intersection JSON not found: %s", jsonPath)
	}

	return dt.loadMetadataFromJSON(jsonPath)
}

// loadMetadataFromJSON reads the CampaignIntersectionOutput JSON and extracts device metadata
func (dt *DeviceTracker) loadMetadataFromJSON(jsonPath string) (map[string]CampaignMetadata, error) {
	startTime := time.Now()

	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}
	defer file.Close()

	var campaignOutput CampaignIntersectionOutput
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&campaignOutput); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
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

	duration := time.Since(startTime)
	fmt.Printf("  ✅ Loaded metadata for %d unique devices from JSON in %v\n", len(metadata), duration)

	if len(metadata) == 0 {
		return nil, fmt.Errorf("no device metadata found in JSON file")
	}

	return metadata, nil
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
