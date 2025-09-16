package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Config struct {
	CHHost     string
	CHPort     int
	CHDatabase string
	CHUser     string
	CHPassword string
}

type CampaignData struct {
	CampaignID string
	Address    string
	Geometry   string
	Polygon    string
	BBoxMinLat float64
	BBoxMaxLat float64
	BBoxMinLon float64
	BBoxMaxLon float64
}

func main() {
	config := Config{
		CHHost:     "localhost",
		CHPort:     9000,
		CHDatabase: "device_tracking",
		CHUser:     "default",
		CHPassword: "nyros",
	}

	// Replace with your root directory path containing subdirectories with CSV files
	rootDir := "./campaign_data"

	if err := processDirectory(rootDir, config); err != nil {
		log.Fatalf("Error processing directory: %v", err)
	}

	fmt.Println("Campaign data import completed successfully!")
}

func processDirectory(rootDir string, config Config) error {
	// Connect to ClickHouse
	db, err := connectClickHouse(config)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	// Create table if not exists
	if err := createTable(db); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Walk through all subdirectories
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %s: %v", path, err)
			return nil // Continue processing other files
		}

		// Process only CSV files
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".csv") {
			log.Printf("Processing file: %s", path)
			if err := processCSVFile(path, db); err != nil {
				log.Printf("Error processing file %s: %v", path, err)
				// Continue with other files instead of stopping
			}
		}
		return nil
	})

	return err
}

func connectClickHouse(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		config.CHUser,
		config.CHPassword,
		config.CHHost,
		config.CHPort,
		config.CHDatabase)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func createTable(db *sql.DB) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS campaigns (
		campaign_id String,
		address String,
		geometry String,
		created_at DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY (campaign_id, created_at)
	`

	_, err := db.Exec(createTableSQL)
	return err
}

func processCSVFile(filePath string, db *sql.DB) error {
	// Extract campaign ID from filename
	campaignID := extractCampaignID(filepath.Base(filePath))
	if campaignID == "" {
		return fmt.Errorf("could not extract campaign ID from filename: %s", filePath)
	}

	// Open CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header row
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}

	// Find column indices
	addressIndex, geometryIndex := findColumnIndices(headers)
	if addressIndex == -1 || geometryIndex == -1 {
		return fmt.Errorf("required columns not found. Headers: %v", headers)
	}

	// Prepare batch insert
	var campaignData []CampaignData

	// Read data rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV row: %v", err)
			continue
		}

		// Skip rows that don't have enough columns
		if len(record) <= addressIndex || len(record) <= geometryIndex {
			continue
		}

		address := strings.TrimSpace(record[addressIndex])
		geometry := strings.TrimSpace(record[geometryIndex])

		// Skip empty rows
		if address == "" || geometry == "" {
			continue
		}

		// Parse polygon and calculate bounding box
		polygon, bbox, err := parsePolygonAndBBox(geometry)
		if err != nil {
			log.Printf("Error parsing geometry for address %s: %v", address, err)
			continue
		}

		campaignData = append(campaignData, CampaignData{
			CampaignID: campaignID,
			Address:    address,
			Geometry:   geometry,
			Polygon:    polygon,
			BBoxMinLat: bbox.MinLat,
			BBoxMaxLat: bbox.MaxLat,
			BBoxMinLon: bbox.MinLon,
			BBoxMaxLon: bbox.MaxLon,
		})
	}

	// Insert data into ClickHouse
	if len(campaignData) > 0 {
		if err := insertCampaignData(db, campaignData); err != nil {
			return fmt.Errorf("failed to insert data: %v", err)
		}
		log.Printf("Inserted %d records for campaign: %s", len(campaignData), campaignID)
	}

	return nil
}

// BoundingBox represents the bounding box of a polygon
type BoundingBox struct {
	MinLat, MaxLat, MinLon, MaxLon float64
}

// parsePolygonAndBBox parses WKT POLYGON and extracts bounding box
func parsePolygonAndBBox(wktPolygon string) (string, BoundingBox, error) {
	// Remove POLYGON and parentheses to get coordinate pairs
	re := regexp.MustCompile(`POLYGON\s*\(\s*\((.*?)\)\s*\)`)
	matches := re.FindStringSubmatch(wktPolygon)
	if len(matches) < 2 {
		return "", BoundingBox{}, fmt.Errorf("invalid POLYGON format: %s", wktPolygon)
	}

	coordsStr := matches[1]
	coordPairs := strings.Split(coordsStr, ",")

	var minLat, maxLat, minLon, maxLon float64
	var points []string

	for i, pair := range coordPairs {
		coords := strings.Fields(strings.TrimSpace(pair))
		if len(coords) != 2 {
			continue
		}

		lon, err := strconv.ParseFloat(coords[0], 64)
		if err != nil {
			continue
		}
		lat, err := strconv.ParseFloat(coords[1], 64)
		if err != nil {
			continue
		}

		// Add to points array for ClickHouse Polygon format
		points = append(points, fmt.Sprintf("(%.6f, %.6f)", lon, lat))

		// Calculate bounding box
		if i == 0 {
			minLat, maxLat = lat, lat
			minLon, maxLon = lon, lon
		} else {
			minLat = math.Min(minLat, lat)
			maxLat = math.Max(maxLat, lat)
			minLon = math.Min(minLon, lon)
			maxLon = math.Max(maxLon, lon)
		}
	}

	// Format polygon for ClickHouse
	clickhousePolygon := fmt.Sprintf("[%s]", strings.Join(points, ", "))

	bbox := BoundingBox{
		MinLat: minLat,
		MaxLat: maxLat,
		MinLon: minLon,
		MaxLon: maxLon,
	}

	return clickhousePolygon, bbox, nil
}

func extractCampaignID(filename string) string {
	// Remove .csv extension
	name := strings.TrimSuffix(filename, ".csv")

	// Remove "Geocoded_Locations - " prefix if present
	if after, ok := strings.CutPrefix(name, "Geocoded_Locations - "); ok {
		name = after
	}

	// Clean up the campaign ID
	campaignID := strings.TrimSpace(name)

	return campaignID
}

func findColumnIndices(headers []string) (addressIndex, geometryIndex int) {
	addressIndex = -1
	geometryIndex = -1

	for i, header := range headers {
		header = strings.ToLower(strings.TrimSpace(header))
		switch header {
		case "address":
			addressIndex = i
		case "geometry":
			geometryIndex = i
		}
	}

	return addressIndex, geometryIndex
}

func insertCampaignData(db *sql.DB, data []CampaignData) error {
	// Prepare batch insert statement
	insertSQL := `INSERT INTO campaigns (campaign_id, address, geometry) VALUES (?, ?, ?)`

	// Begin transaction for better performance
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statement
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Insert each record
	for _, record := range data {
		_, err := stmt.Exec(record.CampaignID, record.Address, record.Geometry)
		if err != nil {
			return fmt.Errorf("failed to insert record: %v", err)
		}
	}

	// Commit transaction
	return tx.Commit()
}
