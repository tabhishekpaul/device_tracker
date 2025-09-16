package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	ClickHouse ClickHouseConfig `json:"clickhouse"`
	Processing ProcessingConfig `json:"processing"`
}

type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ProcessingConfig struct {
	FilterInTime      string  `json:"filter_in_time"`
	FilterOutTime     string  `json:"filter_out_time"`
	MovementThreshold float64 `json:"movement_threshold"`
	SearchRadius      float64 `json:"search_radius_meters"`
	MinIdlePings      int     `json:"min_idle_pings"`
	MaxWorkers        int     `json:"max_workers"`
	BatchSize         int     `json:"batch_size"`
}

type DeviceTracker struct {
	conn   driver.Conn
	config Config
	logger *log.Logger
}

type IdleDevice struct {
	DeviceID     string
	CampaignID   string
	CampaignAddr string
	Latitude     float64
	Longitude    float64
	VisitedTime  time.Time
	PingCount    int64
}

type ConsumerMatch struct {
	DeviceID    string `json:"device_id"`
	DateVisited string `json:"date_visited"`
	TimeVisited string `json:"time_visited"`
	Name        string `json:"name"`
	Address     string `json:"address"`
	Email       string `json:"email"`
	POI         string `json:"poi"`
	Campaign    string `json:"campaign"`
	CityName    string `json:"city_name"`
	State       string `json:"state"`
	ZipCode     string `json:"zip_code"`
}

func NewDeviceTracker(config Config) (*DeviceTracker, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.ClickHouse.Host, config.ClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: config.ClickHouse.Database,
			Username: config.ClickHouse.Username,
			Password: config.ClickHouse.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":                   7200,
			"max_memory_usage":                     80000000000,
			"max_bytes_before_external_group_by":   30000000000,
			"max_bytes_before_external_sort":       30000000000,
			"join_algorithm":                       "hash",
			"max_block_size":                       65536,
			"preferred_block_size_bytes":           1000000,
			"join_use_nulls":                       1,
			"enable_optimize_predicate_expression": 0,
			"max_threads":                          16,
		},
		DialTimeout: 60 * time.Second,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	logger := log.New(os.Stdout, "[DeviceTracker] ", log.LstdFlags)

	return &DeviceTracker{
		conn:   conn,
		config: config,
		logger: logger,
	}, nil
}

func (dt *DeviceTracker) RunCampaignConsumerAnalysis(ctx context.Context, targetDates []string) error {
	startTime := time.Now()
	dt.logger.Println("Starting campaign-based consumer analysis...")

	// Step 1: Find devices within campaign polygons during 2am-6am
	idleDevices, err := dt.findIdleDevicesInCampaigns(ctx, targetDates)
	if err != nil {
		return fmt.Errorf("failed to find idle devices in campaigns: %w", err)
	}

	dt.logger.Printf("Found %d idle devices in campaign areas", len(idleDevices))

	if len(idleDevices) == 0 {
		dt.logger.Println("No idle devices found in campaign areas. Exiting.")
		return nil
	}

	// Step 2: Find consumers within radius of idle devices
	matches, err := dt.findConsumersNearIdleDevices(ctx, idleDevices)
	if err != nil {
		return fmt.Errorf("failed to find consumers near idle devices: %w", err)
	}

	dt.logger.Printf("Found %d consumer matches", len(matches))

	// Step 3: Export results
	err = dt.exportResults(matches)
	if err != nil {
		return fmt.Errorf("failed to export results: %w", err)
	}

	processingTime := time.Since(startTime)
	dt.logger.Printf("Campaign consumer analysis completed in %v", processingTime)

	return nil
}

func (dt *DeviceTracker) findIdleDevicesInCampaigns(ctx context.Context, targetDates []string) ([]IdleDevice, error) {
	dateFilter := "'" + strings.Join(targetDates, "','") + "'"

	query := fmt.Sprintf(`
	WITH 
	-- Step 1: Filter device data by date and time (2am-6am)
	time_filtered_devices AS (
		SELECT 
			device_id,
			event_timestamp,
			latitude,
			longitude,
			load_date
		FROM device_data
		WHERE load_date IN (%s)
		  AND toHour(event_timestamp) >= 2 AND toHour(event_timestamp) < 6
	),
	
	-- Step 2: Find devices within campaign polygons
	devices_in_campaigns AS (
		SELECT 
			tfd.device_id,
			tfd.event_timestamp,
			tfd.latitude,
			tfd.longitude,
			c.campaign_id,
			c.address as campaign_address
		FROM time_filtered_devices tfd
		CROSS JOIN campaigns c
		WHERE pointInPolygon((tfd.longitude, tfd.latitude), c.polygon)
	),
	
	-- Step 3: Identify idle devices
	device_movement_analysis AS (
		SELECT 
			device_id,
			campaign_id,
			campaign_address,
			avg(latitude) as avg_latitude,
			avg(longitude) as avg_longitude,
			min(event_timestamp) as first_seen,
			count(*) as ping_count,
			stddevPop(latitude) as lat_stddev,
			stddevPop(longitude) as lng_stddev
		FROM devices_in_campaigns
		GROUP BY device_id, campaign_id, campaign_address
	),
	
	-- Step 4: Filter for truly idle devices
	idle_devices_final AS (
		SELECT 
			device_id,
			campaign_id,
			campaign_address,
			avg_latitude as latitude,
			avg_longitude as longitude,
			first_seen as visited_time,
			ping_count
		FROM device_movement_analysis
		WHERE ping_count >= %d
		  AND (lat_stddev IS NULL OR lat_stddev < %f)
		  AND (lng_stddev IS NULL OR lng_stddev < %f)
	)
	
	SELECT 
		device_id,
		campaign_id,
		campaign_address,
		latitude,
		longitude,
		visited_time,
		ping_count
	FROM idle_devices_final
	ORDER BY visited_time DESC`,
		dateFilter,
		dt.config.Processing.MinIdlePings,
		dt.config.Processing.MovementThreshold,
		dt.config.Processing.MovementThreshold)

	dt.logger.Println("Executing query to find idle devices in campaigns...")

	rows, err := dt.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute idle devices query: %w", err)
	}
	defer rows.Close()

	var idleDevices []IdleDevice
	for rows.Next() {
		var device IdleDevice
		var visitedTime time.Time

		err := rows.Scan(
			&device.DeviceID,
			&device.CampaignID,
			&device.CampaignAddr,
			&device.Latitude,
			&device.Longitude,
			&visitedTime,
			&device.PingCount,
		)
		if err != nil {
			dt.logger.Printf("Warning: Failed to scan idle device row: %v", err)
			continue
		}

		device.VisitedTime = visitedTime
		idleDevices = append(idleDevices, device)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return idleDevices, nil
}

func (dt *DeviceTracker) findConsumersNearIdleDevices(ctx context.Context, idleDevices []IdleDevice) ([]ConsumerMatch, error) {
	if len(idleDevices) == 0 {
		return []ConsumerMatch{}, nil
	}

	// Check if consumers table has valid coordinates
	var validConsumerCount uint64
	checkQuery := `SELECT count() FROM consumers WHERE latitude != 0 AND longitude != 0 AND latitude IS NOT NULL AND longitude IS NOT NULL`

	row := dt.conn.QueryRow(ctx, checkQuery)
	if err := row.Scan(&validConsumerCount); err != nil {
		dt.logger.Printf("Warning: Failed to check valid consumer coordinates: %v", err)
		validConsumerCount = 0
	}

	dt.logger.Printf("Found %d consumers with valid coordinates", validConsumerCount)

	if validConsumerCount == 0 {
		dt.logger.Println("No consumers with valid coordinates found")
		// Return idle devices without consumer matches
		var matches []ConsumerMatch
		for _, device := range idleDevices {
			match := ConsumerMatch{
				DeviceID:    device.DeviceID,
				DateVisited: device.VisitedTime.Format("2006-01-02"),
				TimeVisited: device.VisitedTime.Format("15:04:05"),
				Name:        "",
				Address:     "",
				Email:       "",
				POI:         device.CampaignAddr,
				Campaign:    device.CampaignID,
				CityName:    "",
				State:       "",
				ZipCode:     "",
			}
			matches = append(matches, match)
		}
		return matches, nil
	}

	// Build device location conditions for the query
	var deviceConditions []string
	for _, device := range idleDevices {
		condition := fmt.Sprintf("(%.6f, %.6f, '%s', '%s', '%s', '%s')",
			device.Latitude, device.Longitude, device.DeviceID, device.CampaignID,
			device.CampaignAddr, device.VisitedTime.Format("2006-01-02 15:04:05"))
		deviceConditions = append(deviceConditions, condition)
	}

	query := fmt.Sprintf(`
	WITH device_locations AS (
		SELECT * FROM VALUES('latitude Float64, longitude Float64, device_id String, campaign_id String, campaign_addr String, visited_time String',
			%s
		)
	)
	SELECT 
		dl.device_id,
		dl.campaign_id,
		dl.campaign_addr,
		dl.visited_time,
		c.id as consumer_id,
		concat(c.PersonFirstName, ' ', c.PersonLastName) as name,
		c.PrimaryAddress as address,
		c.Email as email,
		c.CityName as city,
		c.State as state,
		c.ZipCode as zip_code,
		geoDistance(dl.longitude, dl.latitude, c.longitude, c.latitude) as distance_meters
	FROM device_locations dl
	INNER JOIN consumers c ON (
		c.latitude != 0 AND c.longitude != 0 
		AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
		AND geoDistance(dl.longitude, dl.latitude, c.longitude, c.latitude) <= %f
	)
	ORDER BY dl.campaign_id, dl.device_id, distance_meters`,
		strings.Join(deviceConditions, ","),
		dt.config.Processing.SearchRadius)

	dt.logger.Println("Executing query to find consumers near idle devices...")

	rows, err := dt.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute consumer search query: %w", err)
	}
	defer rows.Close()

	var matches []ConsumerMatch
	for rows.Next() {
		var deviceID, campaignID, campaignAddr, visitedTimeStr string
		var consumerID int64
		var name, address, email, city, state, zipCode string
		var distance float64

		err := rows.Scan(
			&deviceID, &campaignID, &campaignAddr, &visitedTimeStr,
			&consumerID, &name, &address, &email, &city, &state, &zipCode, &distance,
		)
		if err != nil {
			dt.logger.Printf("Warning: Failed to scan consumer match row: %v", err)
			continue
		}

		// Parse visited time
		visitedTime, err := time.Parse("2006-01-02 15:04:05", visitedTimeStr)
		if err != nil {
			dt.logger.Printf("Warning: Failed to parse visited time %s: %v", visitedTimeStr, err)
			visitedTime = time.Now()
		}

		match := ConsumerMatch{
			DeviceID:    deviceID,
			DateVisited: visitedTime.Format("2006-01-02"),
			TimeVisited: visitedTime.Format("15:04:05"),
			Name:        name,
			Address:     address,
			Email:       email,
			POI:         campaignAddr,
			Campaign:    campaignID,
			CityName:    city,
			State:       state,
			ZipCode:     zipCode,
		}
		matches = append(matches, match)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return matches, nil
}

func (dt *DeviceTracker) exportResults(matches []ConsumerMatch) error {
	outputDir := "./output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Group matches by campaign
	campaignMatches := make(map[string][]ConsumerMatch)
	for _, match := range matches {
		campaignID := match.Campaign
		if campaignID == "" {
			campaignID = "unknown_campaign"
		}
		campaignMatches[campaignID] = append(campaignMatches[campaignID], match)
	}

	dt.logger.Printf("Exporting results for %d campaigns", len(campaignMatches))

	// Export results for each campaign
	for campaignID, campaignData := range campaignMatches {
		if err := dt.exportCampaignResults(outputDir, campaignID, campaignData); err != nil {
			dt.logger.Printf("Warning: Failed to export results for campaign %s: %v", campaignID, err)
			continue
		}
	}

	// Also create a combined file
	if err := dt.exportCombinedResults(outputDir, matches); err != nil {
		return fmt.Errorf("failed to export combined results: %w", err)
	}

	return nil
}

func (dt *DeviceTracker) exportCampaignResults(outputDir, campaignID string, matches []ConsumerMatch) error {
	// Create campaign subdirectory
	campaignDir := filepath.Join(outputDir, campaignID)
	if err := os.MkdirAll(campaignDir, 0755); err != nil {
		return fmt.Errorf("failed to create campaign directory: %w", err)
	}

	// Export CSV
	csvPath := filepath.Join(campaignDir, fmt.Sprintf("%s_consumers.csv", campaignID))
	if err := dt.exportCSV(csvPath, matches); err != nil {
		return fmt.Errorf("failed to export CSV for campaign %s: %w", campaignID, err)
	}

	// Export JSON
	jsonPath := filepath.Join(campaignDir, fmt.Sprintf("%s_consumers.json", campaignID))
	if err := dt.exportJSON(jsonPath, matches); err != nil {
		return fmt.Errorf("failed to export JSON for campaign %s: %w", campaignID, err)
	}

	dt.logger.Printf("Exported %d records for campaign %s", len(matches), campaignID)
	return nil
}

func (dt *DeviceTracker) exportCombinedResults(outputDir string, matches []ConsumerMatch) error {
	// Export combined CSV
	csvPath := filepath.Join(outputDir, "Device_Tracker_Output.csv")
	if err := dt.exportCSV(csvPath, matches); err != nil {
		return fmt.Errorf("failed to export combined CSV: %w", err)
	}

	// Export combined JSON
	jsonPath := filepath.Join(outputDir, "Device_Tracker_Output.json")
	if err := dt.exportJSON(jsonPath, matches); err != nil {
		return fmt.Errorf("failed to export combined JSON: %w", err)
	}

	dt.logger.Printf("Exported %d total records to combined files", len(matches))
	return nil
}

func (dt *DeviceTracker) exportCSV(filePath string, matches []ConsumerMatch) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"DeviceID", "Date visited", "Time visited", "Name", "Address",
		"Email", "POI", "Campaign", "CityName", "State", "ZipCode",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, match := range matches {
		record := []string{
			match.DeviceID, match.DateVisited, match.TimeVisited, match.Name,
			match.Address, match.Email, match.POI, match.Campaign,
			match.CityName, match.State, match.ZipCode,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

func (dt *DeviceTracker) exportJSON(filePath string, matches []ConsumerMatch) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	// Create JSON structure
	output := struct {
		Timestamp    string          `json:"timestamp"`
		TotalRecords int             `json:"total_records"`
		Data         []ConsumerMatch `json:"data"`
	}{
		Timestamp:    time.Now().Format("2006-01-02T15:04:05Z07:00"),
		TotalRecords: len(matches),
		Data:         matches,
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}

func (dt *DeviceTracker) Close() error {
	return dt.conn.Close()
}
