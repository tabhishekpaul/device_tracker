package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

// ============================================================================
// STEP 3: IDLE DEVICE DETECTION - OPTIMIZED FOR PERFORMANCE
// ============================================================================

// CampaignMetadata holds the campaign info for a device
type CampaignMetadata struct {
	DeviceID       string
	EventTimestamp time.Time
	Address        string
	Campaign       string
	CampaignID     string
	POIID          string
}

// IdleDeviceResult represents the final idle device output
type IdleDeviceResult struct {
	DeviceID    string
	VisitedTime string
	Address     string
	Campaign    string
	CampaignID  string
	POIID       string
	Geometry    string
}

// ============================================================================
// MAIN ORCHESTRATION METHOD
// ============================================================================

// RunIdleDeviceSearch orchestrates the idle device search for multiple dates
func (dt *DeviceTracker) RunIdleDeviceSearch(folderList []string, targetDates []string) error {
	fmt.Println("\n╔════════════════════════════════════════════════════════╗")
	fmt.Println("║       STEP 3: IDLE DEVICE DETECTION                    ║")
	fmt.Println("╚════════════════════════════════════════════════════════╝")
	startTime := time.Now()

	for _, targetDate := range targetDates {
		fmt.Printf("\n🔍 Processing Date: %s\n", targetDate)
		err := dt.FindIdleDevices(folderList, targetDate)
		if err != nil {
			return fmt.Errorf("failed to find idle devices for %s: %w", targetDate, err)
		}
	}

	err := dt.MergeIdleDevicesOutput(targetDates)
	if err != nil {
		return fmt.Errorf("failed to merge idle devices output: %w", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("\n✅ Step 3 Completed in %v\n", duration)
	return nil
}

// ============================================================================
// CORE PROCESSING METHODS
// ============================================================================

// FindIdleDevices processes time-filtered parquet files for a specific target date
func (dt *DeviceTracker) FindIdleDevices(folderList []string, targetDate string) error {
	// Load campaign metadata
	campaignMetadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return fmt.Errorf("failed to get unique ID dataframe: %w", err)
	}
	fmt.Printf("  📋 Loaded metadata for %d devices\n", len(campaignMetadata))

	// Load all time-filtered parquet files in parallel
	deviceRecords, failedFiles := dt.loadTimeFilteredParquets(folderList, targetDate)

	if len(deviceRecords) == 0 {
		fmt.Printf("  ⚠️  No time-filtered data found for %s\n", targetDate)
		return nil
	}

	fmt.Printf("  📊 Total records loaded: %d\n", len(deviceRecords))

	// Filter idle devices
	err = dt.FilterIdleDevices(targetDate, deviceRecords, campaignMetadata)
	if err != nil {
		return fmt.Errorf("failed to filter idle devices: %w", err)
	}

	if len(failedFiles) > 0 {
		fmt.Println("\n  ⚠️  Failed files:")
		for _, f := range failedFiles {
			fmt.Printf("    - %s\n", f)
		}
	}

	return nil
}

// FilterIdleDevices identifies devices that remain within buffer zone
func (dt *DeviceTracker) FilterIdleDevices(
	targetDate string,
	deviceRecords []DeviceRecord,
	campaignMetadata map[string]CampaignMetadata,
) error {
	outCSV := filepath.Join(
		dt.OutputFolder,
		fmt.Sprintf("Target_Idle_Devices_%s.csv", targetDate),
	)

	// Remove existing file
	os.Remove(outCSV)

	// Group by device_id (optimized with pre-allocated map)
	deviceGroups := dt.groupByDeviceIDOptimized(deviceRecords)
	fmt.Printf("  👥 Unique devices to analyze: %d\n", len(deviceGroups))

	// Process in parallel using worker pool
	idleDevices := dt.processDeviceGroupsParallel(deviceGroups, campaignMetadata)

	fmt.Printf("  ✅ Idle devices found: %d\n", len(idleDevices))

	if len(idleDevices) == 0 {
		fmt.Println("  ℹ️  No idle devices detected")
		return nil
	}

	// Write results to CSV
	err := dt.writeIdleDevicesToCSV(outCSV, idleDevices)
	if err != nil {
		return fmt.Errorf("failed to write CSV: %w", err)
	}

	// Remove duplicates
	err = dt.RemoveCsvDuplicates(outCSV)
	if err != nil {
		return fmt.Errorf("failed to remove duplicates: %w", err)
	}

	fmt.Printf("  💾 Saved to: %s\n", outCSV)
	return nil
}

// MergeIdleDevicesOutput combines results from multiple dates
func (dt *DeviceTracker) MergeIdleDevicesOutput(targetDates []string) error {
	fmt.Println("\n📦 Merging idle device outputs...")

	outCSV := filepath.Join(dt.OutputFolder, "Target_Idle_Devices.csv")

	allRecords := make([][]string, 0)
	var header []string

	for _, targetDate := range targetDates {
		inCSV := filepath.Join(
			dt.OutputFolder,
			fmt.Sprintf("Target_Idle_Devices_%s.csv", targetDate),
		)

		if records, h, err := dt.readCSVFile(inCSV); err == nil {
			if header == nil {
				header = h
			}
			allRecords = append(allRecords, records...)
			fmt.Printf("  ✓ Loaded %d records from %s\n", len(records), targetDate)
		}
	}

	if len(allRecords) == 0 {
		return fmt.Errorf("no idle device records found")
	}

	// Remove duplicates by device_id and campaign
	uniqueRecords := dt.removeDuplicatesByKeys(allRecords, []int{0, 3}) // device_id, campaign
	fmt.Printf("  📊 Total: %d → Unique: %d\n", len(allRecords), len(uniqueRecords))

	// Write merged output
	err := dt.writeCSVFile(outCSV, header, uniqueRecords)
	if err != nil {
		return fmt.Errorf("failed to write merged output: %w", err)
	}

	fmt.Printf("  💾 Merged output: %s\n", outCSV)
	return nil
}

// ============================================================================
// OPTIMIZED HELPER METHODS
// ============================================================================

// GetUniqIdDataFrame loads unique device metadata (optimized with minimal allocations)
func (dt *DeviceTracker) GetUniqIdDataFrame() (map[string]CampaignMetadata, error) {
	csvPath := filepath.Join(dt.OutputFolder, "Devices_Within_Campaign.csv")

	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("campaign devices CSV not found: %s", csvPath)
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true // OPTIMIZATION: Reuse record slice

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Find column indices
	colIndices := make(map[string]int)
	for i, col := range header {
		colIndices[col] = i
	}

	metadata := make(map[string]CampaignMetadata, 100000) // Pre-allocate

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		deviceID := record[colIndices["device_id"]]

		// Only keep first occurrence
		if _, exists := metadata[deviceID]; exists {
			continue
		}

		timestamp, _ := time.Parse(time.RFC3339, record[colIndices["event_timestamp"]])

		metadata[deviceID] = CampaignMetadata{
			DeviceID:       deviceID,
			EventTimestamp: timestamp,
			Address:        record[colIndices["address"]],
			Campaign:       record[colIndices["campaign"]],
			CampaignID:     record[colIndices["campaign_id"]],
			POIID:          record[colIndices["poi_id"]],
		}
	}

	return metadata, nil
}

// loadTimeFilteredParquets loads parquet files in parallel
func (dt *DeviceTracker) loadTimeFilteredParquets(folderList []string, targetDate string) ([]DeviceRecord, []string) {
	type result struct {
		records []DeviceRecord
		failed  string
	}

	resultChan := make(chan result, len(folderList))
	var wg sync.WaitGroup

	// Load files in parallel
	for _, folderName := range folderList {
		wg.Add(1)
		go func(folder string) {
			defer wg.Done()

			parquetPath := filepath.Join(
				dt.OutputFolder,
				"time_filtered",
				strings.TrimPrefix(folder, "load_date="),
				fmt.Sprintf("time_filtered_loaddate%s.parquet", strings.TrimPrefix(folder, "load_date=")),
			)

			if _, err := os.Stat(parquetPath); err == nil {
				records, err := dt.readTimeFilteredParquet(parquetPath)
				if err != nil {
					resultChan <- result{failed: parquetPath}
				} else {
					resultChan <- result{records: records}
				}
			}
		}(folderName)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	allRecords := make([]DeviceRecord, 0, 1000000)
	failedFiles := make([]string, 0)

	for res := range resultChan {
		if res.failed != "" {
			failedFiles = append(failedFiles, res.failed)
		} else {
			allRecords = append(allRecords, res.records...)
		}
	}

	return allRecords, failedFiles
}

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
// CSV HELPER METHODS
// ============================================================================

// writeIdleDevicesToCSV writes idle devices to CSV efficiently
func (dt *DeviceTracker) writeIdleDevicesToCSV(filePath string, devices []IdleDeviceResult) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ','
	defer writer.Flush()

	// Write header
	header := []string{"device_id", "visited_time", "address", "campaign", "campaign_id", "poi_id", "geometry"}
	writer.Write(header)

	// Write data
	for i := range devices {
		row := []string{
			devices[i].DeviceID,
			devices[i].VisitedTime,
			devices[i].Address,
			devices[i].Campaign,
			devices[i].CampaignID,
			devices[i].POIID,
			devices[i].Geometry,
		}
		writer.Write(row)
	}

	return nil
}

// readCSVFile reads CSV and returns records without header
func (dt *DeviceTracker) readCSVFile(filePath string) ([][]string, []string, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	if len(records) < 2 {
		return nil, nil, nil
	}

	return records[1:], records[0], nil
}

// writeCSVFile writes header and records to CSV
func (dt *DeviceTracker) writeCSVFile(filePath string, header []string, records [][]string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write(header)
	writer.WriteAll(records)

	return nil
}

// removeDuplicatesByKeys removes duplicates based on specified column indices
func (dt *DeviceTracker) removeDuplicatesByKeys(records [][]string, keyIndices []int) [][]string {
	seen := make(map[string]bool, len(records))
	unique := make([][]string, 0, len(records))

	for _, record := range records {
		// Build key from specified columns
		keyParts := make([]string, len(keyIndices))
		for i, idx := range keyIndices {
			if idx < len(record) {
				keyParts[i] = record[idx]
			}
		}
		key := strings.Join(keyParts, "|")

		if !seen[key] {
			seen[key] = true
			unique = append(unique, record)
		}
	}

	return unique
}

// RemoveCsvDuplicates removes duplicate rows from CSV file
func (dt *DeviceTracker) RemoveCsvDuplicates(csvPath string) error {
	records, header, err := dt.readCSVFile(csvPath)
	if err != nil || len(records) == 0 {
		return err
	}

	// Remove exact duplicates
	seen := make(map[string]bool, len(records))
	unique := make([][]string, 0, len(records))

	for _, record := range records {
		key := strings.Join(record, "|")
		if !seen[key] {
			seen[key] = true
			unique = append(unique, record)
		}
	}

	// Remove duplicates by device_id (keep first)
	seenDevices := make(map[string]bool, len(unique))
	finalUnique := make([][]string, 0, len(unique))

	for _, record := range unique {
		if len(record) > 0 {
			deviceID := record[0]
			if !seenDevices[deviceID] {
				seenDevices[deviceID] = true
				finalUnique = append(finalUnique, record)
			}
		}
	}

	return dt.writeCSVFile(csvPath, header, finalUnique)
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
