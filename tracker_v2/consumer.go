package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

const (
	MatchRadiusMeters = 30.0
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
}

type CoordinateStats struct {
	TotalConsumers    int64
	ValidCoords       int64
	InvalidZeroZero   int64
	OutOfBoundsLat    int64
	OutOfBoundsLon    int64
	MinLat, MaxLat    float64
	MinLon, MaxLon    float64
	SampleValidCoords []ConsumerRecord
}

type ConsumerDeviceMatcher struct {
	outputFolder       string
	consumerFolder     string
	idleDevicesPath    string
	logger             *log.Logger
	deviceSpatialIndex map[int64][]IdleDevice
	stats              CoordinateStats
}

func NewConsumerDeviceMatcher(outputFolder, consumerFolder, idleDevicesPath string) *ConsumerDeviceMatcher {
	return &ConsumerDeviceMatcher{
		outputFolder:       outputFolder,
		consumerFolder:     consumerFolder,
		idleDevicesPath:    idleDevicesPath,
		logger:             log.New(os.Stdout, "[DETAILED-DEBUG] ", log.LstdFlags),
		deviceSpatialIndex: make(map[int64][]IdleDevice),
		stats: CoordinateStats{
			MinLat:            90.0,
			MaxLat:            -90.0,
			MinLon:            180.0,
			MaxLon:            -180.0,
			SampleValidCoords: make([]ConsumerRecord, 0, 10),
		},
	}
}

func getCellKey(lat, lon float64) int64 {
	cellX := int64(lon / GridSize)
	cellY := int64(lat / GridSize)
	return cellX*1000000 + cellY
}

func (cdm *ConsumerDeviceMatcher) loadIdleDevices() error {
	cdm.logger.Println("╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  LOADING IDLE DEVICES                                 ║")
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

	for _, devices := range idleData.IdleDevicesByDate {
		for _, device := range devices {
			lat, lon := cdm.parsePoint(device.Geometry)

			idleDevice := IdleDevice{
				DeviceID:    device.DeviceID,
				VisitedTime: device.VisitedTime,
				Address:     device.Address,
				Campaign:    device.Campaign,
				Latitude:    lat,
				Longitude:   lon,
			}

			cellKey := getCellKey(lat, lon)
			cdm.deviceSpatialIndex[cellKey] = append(cdm.deviceSpatialIndex[cellKey], idleDevice)
			totalDevices++
		}
	}

	cdm.logger.Printf("✅ Loaded %d idle devices in %d grid cells\n", totalDevices, len(cdm.deviceSpatialIndex))
	return nil
}

func (cdm *ConsumerDeviceMatcher) parsePoint(geometryStr string) (float64, float64) {
	geometryStr = strings.TrimPrefix(geometryStr, "POINT (")
	geometryStr = strings.TrimSuffix(geometryStr, ")")

	var lon, lat float64
	fmt.Sscanf(geometryStr, "%f %f", &lon, &lat)
	return lat, lon
}

func (cdm *ConsumerDeviceMatcher) analyzeConsumerCoordinates() error {
	cdm.logger.Println("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  ANALYZING CONSUMER COORDINATES                       ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

	patterns := []string{
		filepath.Join(cdm.consumerFolder, "consumer_raw_batch_*.parquet"),
		filepath.Join(cdm.consumerFolder, "consumers_chunk_*.parquet"),
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

	cdm.logger.Printf("\nFound %d consumer parquet files\n", len(files))
	cdm.logger.Println("Analyzing ALL files for coordinate distribution...\n")

	// Process all files to get complete statistics
	for idx, fpath := range files {
		if idx%10 == 0 {
			cdm.logger.Printf("Progress: %d/%d files analyzed...", idx, len(files))
		}

		if err := cdm.analyzeFile(fpath); err != nil {
			cdm.logger.Printf("Warning: error analyzing %s: %v", filepath.Base(fpath), err)
		}
	}

	// Print detailed statistics
	cdm.printStatistics()

	return nil
}

func (cdm *ConsumerDeviceMatcher) analyzeFile(fpath string) error {
	f, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 10000}, memory.NewGoAllocator())
	if err != nil {
		return err
	}

	recordReader, err := arrowReader.GetRecordReader(context.TODO(), nil, nil)
	if err != nil {
		return err
	}
	defer recordReader.Release()

	for recordReader.Next() {
		rec := recordReader.Record()
		rows := int(rec.NumRows())
		fields := rec.Schema().Fields()

		colIndex := make(map[string]int)
		for i, f := range fields {
			colIndex[f.Name] = i
		}

		for i := 0; i < rows; i++ {
			cdm.stats.TotalConsumers++

			consumer := ConsumerRecord{}

			// Read ID
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

			// Read coordinates
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

			// Categorize coordinates
			if consumer.Latitude == 0 && consumer.Longitude == 0 {
				cdm.stats.InvalidZeroZero++
			} else if consumer.Latitude < -90 || consumer.Latitude > 90 {
				cdm.stats.OutOfBoundsLat++
			} else if consumer.Longitude < -180 || consumer.Longitude > 180 {
				cdm.stats.OutOfBoundsLon++
			} else {
				cdm.stats.ValidCoords++

				// Track min/max
				if consumer.Latitude < cdm.stats.MinLat {
					cdm.stats.MinLat = consumer.Latitude
				}
				if consumer.Latitude > cdm.stats.MaxLat {
					cdm.stats.MaxLat = consumer.Latitude
				}
				if consumer.Longitude < cdm.stats.MinLon {
					cdm.stats.MinLon = consumer.Longitude
				}
				if consumer.Longitude > cdm.stats.MaxLon {
					cdm.stats.MaxLon = consumer.Longitude
				}

				// Save first 10 valid samples
				if len(cdm.stats.SampleValidCoords) < 10 {
					// Read other fields for sample
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
					cdm.stats.SampleValidCoords = append(cdm.stats.SampleValidCoords, consumer)
				}
			}
		}
		rec.Release()
	}

	return nil
}

func (cdm *ConsumerDeviceMatcher) printStatistics() {
	cdm.logger.Println("\n╔═══════════════════════════════════════════════════════╗")
	cdm.logger.Println("║  COORDINATE ANALYSIS RESULTS                          ║")
	cdm.logger.Println("╚═══════════════════════════════════════════════════════╝")

	totalInvalid := cdm.stats.InvalidZeroZero + cdm.stats.OutOfBoundsLat + cdm.stats.OutOfBoundsLon

	cdm.logger.Printf("\n📊 OVERALL STATISTICS:")
	cdm.logger.Printf("   Total consumers:          %d", cdm.stats.TotalConsumers)
	cdm.logger.Printf("   Valid coordinates:        %d (%.2f%%)",
		cdm.stats.ValidCoords,
		float64(cdm.stats.ValidCoords)*100/float64(cdm.stats.TotalConsumers))
	cdm.logger.Printf("   Invalid coordinates:      %d (%.2f%%)",
		totalInvalid,
		float64(totalInvalid)*100/float64(cdm.stats.TotalConsumers))

	cdm.logger.Printf("\n📉 BREAKDOWN OF INVALID:")
	cdm.logger.Printf("   (0, 0) coordinates:       %d (%.2f%%)",
		cdm.stats.InvalidZeroZero,
		float64(cdm.stats.InvalidZeroZero)*100/float64(cdm.stats.TotalConsumers))
	cdm.logger.Printf("   Out of bounds latitude:   %d", cdm.stats.OutOfBoundsLat)
	cdm.logger.Printf("   Out of bounds longitude:  %d", cdm.stats.OutOfBoundsLon)

	if cdm.stats.ValidCoords > 0 {
		cdm.logger.Printf("\n📍 COORDINATE BOUNDS:")
		cdm.logger.Printf("   Latitude range:  %.6f to %.6f", cdm.stats.MinLat, cdm.stats.MaxLat)
		cdm.logger.Printf("   Longitude range: %.6f to %.6f", cdm.stats.MinLon, cdm.stats.MaxLon)

		cdm.logger.Printf("\n✅ SAMPLE VALID CONSUMERS:")
		for idx, c := range cdm.stats.SampleValidCoords {
			cdm.logger.Printf("   Sample #%d:", idx+1)
			cdm.logger.Printf("     ID:       %d", c.ID)
			cdm.logger.Printf("     Name:     %s %s", c.PersonFirstName, c.PersonLastName)
			cdm.logger.Printf("     Address:  %s, %s %s", c.PrimaryAddress, c.CityName, c.State)
			cdm.logger.Printf("     Lat/Lon:  %.6f, %.6f", c.Latitude, c.Longitude)
			cdm.logger.Printf("     Grid Cell: %d", getCellKey(c.Latitude, c.Longitude))
		}

		// Check if valid consumers are in USA (where devices are)
		usaCount := 0
		for _, c := range cdm.stats.SampleValidCoords {
			if c.Latitude >= 24 && c.Latitude <= 50 && c.Longitude >= -125 && c.Longitude <= -65 {
				usaCount++
			}
		}
		cdm.logger.Printf("\n🗺️  GEOGRAPHIC CHECK:")
		cdm.logger.Printf("   Samples in USA bounds: %d/%d", usaCount, len(cdm.stats.SampleValidCoords))
		if usaCount == 0 {
			cdm.logger.Println("   ⚠️  WARNING: Valid consumers may not be in USA where devices are!")
		}
	}

	cdm.logger.Println("\n╚═══════════════════════════════════════════════════════╝")
}

func (cdm *ConsumerDeviceMatcher) Run() error {
	startTime := time.Now()

	if err := cdm.loadIdleDevices(); err != nil {
		return err
	}

	if err := cdm.analyzeConsumerCoordinates(); err != nil {
		return err
	}

	elapsed := time.Since(startTime)

	cdm.logger.Printf("\n⏱️  Analysis completed in %.2f seconds\n", elapsed.Seconds())

	if cdm.stats.ValidCoords > 0 {
		percentage := float64(cdm.stats.ValidCoords) * 100 / float64(cdm.stats.TotalConsumers)
		cdm.logger.Printf("✅ GOOD NEWS: You have %d consumers (%.2f%%) with valid coordinates!",
			cdm.stats.ValidCoords, percentage)
		cdm.logger.Println("   Step 4 matching should work with these consumers.")
		cdm.logger.Println("   Re-run the optimized matcher to find matches!")
	} else {
		cdm.logger.Println("❌ NO VALID COORDINATES: All consumers have invalid lat/lon")
		cdm.logger.Println("   Need to geocode consumer addresses before matching")
	}

	return nil
}
