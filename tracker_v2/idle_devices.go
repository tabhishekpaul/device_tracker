package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/paulmach/orb"
)

// ============================================================================
// DIAGNOSTIC VERSION - Check what's in the parquet file
// ============================================================================

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
// DIAGNOSTIC: Check parquet contents
// ============================================================================

func (dt *DeviceTracker) RunIdleDeviceSearch(folderList []string, targetDates []string) error {
	fmt.Println("\n╔════════════════════════════════════════════════════════╗")
	fmt.Println("║   STEP 3: DIAGNOSTIC MODE                             ║")
	fmt.Println("╚════════════════════════════════════════════════════════╝")

	idleDevicesFolder := filepath.Join(dt.OutputFolder, "idle_devices")
	os.MkdirAll(idleDevicesFolder, 0755)

	for _, targetDate := range targetDates {
		fmt.Printf("\n🔍 Processing Date: %s\n", targetDate)
		err := dt.DiagnosticCheck(folderList, targetDate)
		if err != nil {
			fmt.Printf("  ⚠️  Error: %v\n", err)
			continue
		}
	}

	return nil
}

func (dt *DeviceTracker) DiagnosticCheck(folderList []string, targetDate string) error {
	// Load campaign metadata
	campaignMetadata, err := dt.GetUniqIdDataFrame()
	if err != nil {
		return fmt.Errorf("failed to load campaign metadata: %w", err)
	}
	fmt.Printf("  📋 Campaign metadata devices: %d\n", len(campaignMetadata))

	// Create lookup set
	campaignDeviceSet := make(map[string]bool, len(campaignMetadata))
	for deviceID := range campaignMetadata {
		campaignDeviceSet[deviceID] = true
	}

	// Check time-filtered parquet
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

		fmt.Printf("\n    📂 Checking: %s\n", filepath.Base(parquetPath))

		// Sample first few row groups to see what devices are present
		dt.sampleParquetDevices(parquetPath, campaignDeviceSet, 5)
	}

	return nil
}

func (dt *DeviceTracker) sampleParquetDevices(parquetPath string, campaignDeviceSet map[string]bool, maxRowGroups int) {
	pf, err := file.OpenParquetFile(parquetPath, false)
	if err != nil {
		fmt.Printf("      ⚠️  Failed to open: %v\n", err)
		return
	}
	defer pf.Close()

	numRowGroups := pf.NumRowGroups()
	fmt.Printf("      📊 Total row groups: %d\n", numRowGroups)

	if maxRowGroups > numRowGroups {
		maxRowGroups = numRowGroups
	}

	totalDevices := make(map[string]bool)
	matchingDevices := make(map[string]bool)
	totalRecords := 0

	// Sample first N row groups
	for rgIdx := 0; rgIdx < maxRowGroups; rgIdx++ {
		records, err := dt.readRowGroup(pf, rgIdx)
		if err != nil {
			continue
		}

		fmt.Printf("      🔍 Row group %d: %d records\n", rgIdx+1, len(records))

		for i := range records {
			deviceID := records[i].DeviceID
			totalDevices[deviceID] = true
			totalRecords++

			if campaignDeviceSet[deviceID] {
				matchingDevices[deviceID] = true
			}
		}

		// Show first 5 device IDs as sample
		if rgIdx == 0 && len(records) > 0 {
			fmt.Printf("        Sample device IDs from first row group:\n")
			count := 0
			for i := range records {
				if count >= 5 {
					break
				}
				deviceID := records[i].DeviceID
				inCampaign := "❌ NOT in campaign"
				if campaignDeviceSet[deviceID] {
					inCampaign = "✅ IN campaign"
				}
				fmt.Printf("          - %s %s\n", deviceID, inCampaign)
				count++
			}
		}

		records = nil
		runtime.GC()
	}

	fmt.Printf("\n      📊 SAMPLE ANALYSIS (first %d row groups):\n", maxRowGroups)
	fmt.Printf("         Total records: %d\n", totalRecords)
	fmt.Printf("         Unique devices in sample: %d\n", len(totalDevices))
	fmt.Printf("         Devices matching campaign: %d\n", len(matchingDevices))
	fmt.Printf("         Match rate: %.2f%%\n", float64(len(matchingDevices))/float64(len(totalDevices))*100)

	if len(matchingDevices) == 0 {
		fmt.Printf("\n      ⚠️  WARNING: NO MATCHING DEVICES FOUND!\n")
		fmt.Printf("         This means time-filtered parquet has different devices than campaign metadata.\n")
		fmt.Printf("         Possible causes:\n")
		fmt.Printf("         1. Step 2 (time filter) was run BEFORE Step 1 (campaign intersection)\n")
		fmt.Printf("         2. Different load_date folders were used\n")
		fmt.Printf("         3. Campaign intersection didn't save properly\n")
	}
}

// ============================================================================
// Helper methods
// ============================================================================

func (dt *DeviceTracker) GetUniqIdDataFrame() (map[string]CampaignMetadata, error) {
	yesterday := time.Now().AddDate(0, 0, -1)
	dateStr := yesterday.Format("20060102")

	jsonPath := filepath.Join(dt.OutputFolder, "campaign_intersection", dateStr, fmt.Sprintf("campaign_devices_%s.json", dateStr))

	fmt.Printf("  📄 Reading from: %s\n", jsonPath)

	if _, err := os.Stat(jsonPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("JSON not found: %s", jsonPath)
	}

	return dt.loadMetadataFromJSON(jsonPath)
}

func (dt *DeviceTracker) loadMetadataFromJSON(jsonPath string) (map[string]CampaignMetadata, error) {
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

func (dt *DeviceTracker) MergeIdleDevicesByEventDate() error {
	return nil // Diagnostic mode - skip merge
}
