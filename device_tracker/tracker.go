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
			// Optimized for 16B rows
			"max_execution_time":                       14400,          // 4 hours
			"max_memory_usage":                         "200000000000", // 200GB
			"max_bytes_before_external_group_by":       "100000000000", // 100GB
			"max_bytes_before_external_sort":           "100000000000", // 100GB
			"join_algorithm":                           "hash",
			"max_block_size":                           1048576,  // 1M rows per block
			"preferred_block_size_bytes":               10000000, // 10MB
			"join_use_nulls":                           1,
			"enable_optimize_predicate_expression":     1,
			"max_threads":                              0,             // Use all available cores
			"max_bytes_before_remerge_sort":            "50000000000", // 50GB
			"distributed_aggregation_memory_efficient": 1,
			"optimize_skip_unused_shards":              1,
			"use_index_for_in_with_subqueries":         1,
		},
		DialTimeout: 300 * time.Second, // 5 minutes
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
	dt.logger.Println("Starting optimized single-query campaign consumer analysis...")

	// Execute single comprehensive query
	matches, err := dt.executeOptimizedQuery(ctx, targetDates)
	if err != nil {
		return fmt.Errorf("failed to execute optimized query: %w", err)
	}

	dt.logger.Printf("Found %d consumer matches", len(matches))

	// Export results
	err = dt.exportResults(matches)
	if err != nil {
		return fmt.Errorf("failed to export results: %w", err)
	}

	processingTime := time.Since(startTime)
	dt.logger.Printf("Campaign consumer analysis completed in %v", processingTime)

	return nil
}

func (dt *DeviceTracker) executeOptimizedQuery(ctx context.Context, targetDates []string) ([]ConsumerMatch, error) {
	dateFilter := "'" + strings.Join(targetDates, "','") + "'"

	// Single comprehensive query that does everything server-side
	query := fmt.Sprintf(`
	WITH 
	-- Step 1: Pre-filter device data by date and time (2am-6am) with partitioning
	time_filtered_devices AS (
		SELECT 
			device_id,
			event_timestamp,
			latitude,
			longitude,
			load_date
		FROM device_data
		WHERE load_date IN (%s)
		  AND toHour(event_timestamp) >= 2 
		  AND toHour(event_timestamp) < 6
		  AND latitude != 0 
		  AND longitude != 0
		  AND latitude IS NOT NULL 
		  AND longitude IS NOT NULL
	),
	
	-- Step 2: Find devices within campaign polygons (optimized with spatial index)
	devices_in_campaigns AS (
		SELECT 
			tfd.device_id,
			tfd.event_timestamp,
			tfd.latitude,
			tfd.longitude,
			c.campaign_id,
			c.address as campaign_address
		FROM time_filtered_devices tfd
		INNER JOIN campaigns c ON pointInPolygon((tfd.longitude, tfd.latitude), c.polygon)
	),
	
	-- Step 3: Aggregate and identify idle devices
	idle_devices AS (
		SELECT 
			device_id,
			campaign_id,
			campaign_address,
			avg(latitude) as avg_latitude,
			avg(longitude) as avg_longitude,
			min(event_timestamp) as visited_time,
			count(*) as ping_count,
			stddevPop(latitude) as lat_stddev,
			stddevPop(longitude) as lng_stddev
		FROM devices_in_campaigns
		GROUP BY device_id, campaign_id, campaign_address
		HAVING ping_count >= %d
		   AND (lat_stddev IS NULL OR lat_stddev < %f)
		   AND (lng_stddev IS NULL OR lng_stddev < %f)
	),
	
	-- Step 4: Find consumers near idle devices in single join
	consumer_matches AS (
		SELECT 
			i.device_id,
			i.campaign_id,
			i.campaign_address,
			i.visited_time,
			c.id as consumer_id,
			concat(c.PersonFirstName, ' ', c.PersonLastName) as name,
			c.PrimaryAddress as address,
			c.Email as email,
			c.CityName as city,
			c.State as state,
			c.ZipCode as zip_code,
			geoDistance(i.avg_longitude, i.avg_latitude, c.longitude, c.latitude) as distance_meters,
			-- Add row number to get closest consumer per device
			ROW_NUMBER() OVER (
				PARTITION BY i.device_id, i.campaign_id 
				ORDER BY geoDistance(i.avg_longitude, i.avg_latitude, c.longitude, c.latitude)
			) as rn
		FROM idle_devices i
		INNER JOIN consumers c ON (
			c.latitude != 0 AND c.longitude != 0 
			AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
			AND geoDistance(i.avg_longitude, i.avg_latitude, c.longitude, c.latitude) <= %f
		)
	)
	
	-- Final result: Get only the closest consumer per device
	SELECT 
		device_id,
		formatDateTime(visited_time, '%%Y-%%m-%%d') as date_visited,
		formatDateTime(visited_time, '%%H:%%M:%%S') as time_visited,
		name,
		address,
		email,
		campaign_address as poi,
		campaign_id as campaign,
		city as city_name,
		state,
		zip_code
	FROM consumer_matches
	WHERE rn = 1
	ORDER BY campaign_id, visited_time DESC`,
		dateFilter,
		dt.config.Processing.MinIdlePings,
		dt.config.Processing.MovementThreshold,
		dt.config.Processing.MovementThreshold,
		dt.config.Processing.SearchRadius)

	dt.logger.Println("Executing optimized single query for 16B rows...")
	dt.logger.Printf("Query settings: max_memory=200GB, external_sort/group_by=100GB, timeout=4h")

	// Use a context with timeout for very long queries
	queryCtx, cancel := context.WithTimeout(ctx, 4*time.Hour)
	defer cancel()

	rows, err := dt.conn.Query(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute optimized query: %w", err)
	}
	defer rows.Close()

	var matches []ConsumerMatch
	count := 0

	for rows.Next() {
		var match ConsumerMatch

		err := rows.Scan(
			&match.DeviceID,
			&match.DateVisited,
			&match.TimeVisited,
			&match.Name,
			&match.Address,
			&match.Email,
			&match.POI,
			&match.Campaign,
			&match.CityName,
			&match.State,
			&match.ZipCode,
		)
		if err != nil {
			dt.logger.Printf("Warning: Failed to scan consumer match row: %v", err)
			continue
		}

		matches = append(matches, match)
		count++

		// Progress logging for large datasets
		if count%100000 == 0 {
			dt.logger.Printf("Processed %d records...", count)
		}
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
	// Clean campaign ID for filesystem compatibility
	cleanCampaignID := strings.ReplaceAll(campaignID, " ", "_")
	cleanCampaignID = strings.ReplaceAll(cleanCampaignID, "/", "_")

	// Create campaign subdirectory
	campaignDir := filepath.Join(outputDir, cleanCampaignID)
	if err := os.MkdirAll(campaignDir, 0755); err != nil {
		return fmt.Errorf("failed to create campaign directory: %w", err)
	}

	// Export CSV
	csvPath := filepath.Join(campaignDir, fmt.Sprintf("%s_consumers.csv", cleanCampaignID))
	if err := dt.exportCSV(csvPath, matches); err != nil {
		return fmt.Errorf("failed to export CSV for campaign %s: %w", campaignID, err)
	}

	// Export JSON
	jsonPath := filepath.Join(campaignDir, fmt.Sprintf("%s_consumers.json", cleanCampaignID))
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
