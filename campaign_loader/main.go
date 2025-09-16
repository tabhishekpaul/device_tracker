package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
	CREATE TABLE IF NOT EXISTS device_tracking.campaigns (
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

		campaignData = append(campaignData, CampaignData{
			CampaignID: campaignID,
			Address:    address,
			Geometry:   geometry,
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
	insertSQL := `INSERT INTO device_tracking.campaigns (campaign_id, address, geometry) VALUES (?, ?, ?)`

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

// Alternative batch insert method for better performance with large datasets
func insertCampaignDataBatch(db *sql.DB, data []CampaignData) error {
	if len(data) == 0 {
		return nil
	}

	// Build bulk insert query
	valueStrings := make([]string, 0, len(data))
	valueArgs := make([]interface{}, 0, len(data)*3)

	for _, record := range data {
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, record.CampaignID, record.Address, record.Geometry)
	}

	query := fmt.Sprintf("INSERT INTO device_tracking.campaigns (campaign_id, address, geometry) VALUES %s",
		strings.Join(valueStrings, ","))

	_, err := db.Exec(query, valueArgs...)
	return err
}
