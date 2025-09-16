package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
	Polygon    [][][2]float64 // ClickHouse Polygon type
}

func main() {
	config := Config{
		CHHost:     "localhost",
		CHPort:     9000,
		CHDatabase: "device_tracking",
		CHUser:     "default",
		CHPassword: "nyros",
	}

	rootDir := "/mnt/blobcontainer/Geocoded"

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
			return nil
		}

		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".csv") {
			log.Printf("Processing file: %s", path)
			if err := processCSVFile(path, db); err != nil {
				log.Printf("Error processing file %s: %v", path, err)
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
		polygon Polygon,
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

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}

	addressIndex, geometryIndex := findColumnIndices(headers)
	if addressIndex == -1 || geometryIndex == -1 {
		return fmt.Errorf("required columns not found. Headers: %v", headers)
	}

	var campaignData []CampaignData

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV row: %v", err)
			continue
		}

		if len(record) <= addressIndex || len(record) <= geometryIndex {
			continue
		}

		address := strings.TrimSpace(record[addressIndex])
		geometry := strings.TrimSpace(record[geometryIndex])
		if address == "" || geometry == "" {
			continue
		}

		// Parse WKT polygon into ClickHouse format
		polygon, err := parseWKTPolygon(geometry)
		if err != nil {
			log.Printf("Error parsing geometry for address %s: %v", address, err)
			continue
		}

		campaignData = append(campaignData, CampaignData{
			CampaignID: campaignID,
			Address:    address,
			Geometry:   geometry,
			Polygon:    polygon,
		})
	}

	if len(campaignData) > 0 {
		if err := insertCampaignData(db, campaignData); err != nil {
			return fmt.Errorf("failed to insert data: %v", err)
		}
		log.Printf("Inserted %d records for campaign: %s", len(campaignData), campaignID)
	}

	return nil
}

func parseWKTPolygon(wkt string) ([][][2]float64, error) {
	wkt = strings.TrimSpace(wkt)
	wkt = strings.ToUpper(wkt) // normalize casing just in case

	// Remove POLYGON wrapper safely
	wkt = strings.TrimPrefix(wkt, "POLYGON((")
	wkt = strings.TrimPrefix(wkt, "POLYGON ((")
	wkt = strings.TrimSuffix(wkt, "))")

	coords := strings.Split(wkt, ",")
	points := make([][2]float64, 0, len(coords))

	for _, c := range coords {
		parts := strings.Fields(strings.TrimSpace(c))
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid coord: %s", c)
		}

		x, err1 := strconv.ParseFloat(parts[0], 64)
		y, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("invalid float in coord: %s", c)
		}

		points = append(points, [2]float64{x, y})
	}

	// Return as outer ring
	return [][][2]float64{points}, nil
}

func extractCampaignID(filename string) string {
	name := strings.TrimSuffix(filename, ".csv")
	if strings.HasPrefix(name, "Geocoded_Locations - ") {
		name = strings.TrimPrefix(name, "Geocoded_Locations - ")
	}
	return strings.TrimSpace(name)
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
	insertSQL := `INSERT INTO campaigns (campaign_id, address, geometry, polygon) VALUES (?, ?, ?, ?)`

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range data {
		_, err := stmt.Exec(
			record.CampaignID,
			record.Address,
			record.Geometry,
			record.Polygon, // Proper Polygon type
		)
		if err != nil {
			return fmt.Errorf("failed to insert record: %v", err)
		}
	}

	return tx.Commit()
}
