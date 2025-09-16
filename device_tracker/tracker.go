package main

import (
	"context"
	"encoding/csv"
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
	MinPings          int     `json:"min_pings"`
	MaxWorkers        int     `json:"max_workers"`
	BatchSize         int     `json:"batch_size"`
}

type DeviceTracker struct {
	conn   driver.Conn
	config Config
	logger *log.Logger
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
			"max_memory_usage":                     80000000000, // 50GB
			"max_bytes_before_external_group_by":   30000000000, // 20GB before spilling to disk
			"max_bytes_before_external_sort":       30000000000, // 20GB before spilling to disk
			"join_algorithm":                       "hash",      // Use hash join
			"max_block_size":                       65536,
			"preferred_block_size_bytes":           1000000,
			"join_use_nulls":                       1,
			"enable_optimize_predicate_expression": 0,
			"max_threads":                          16,          // Increase parallelism
			"max_bytes_before_remerge_sort":        30000000000, // 10GB before remerge
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

func (dt *DeviceTracker) RunCompleteAnalysis(ctx context.Context, targetDates []string) error {
	startTime := time.Now()
	dt.logger.Println("Starting single-query complete analysis...")

	// Check consumer data availability
	var consumerCount uint64
	row := dt.conn.QueryRow(ctx, "SELECT count() FROM consumers")
	if err := row.Scan(&consumerCount); err != nil {
		dt.logger.Printf("Warning: Failed to check consumer count: %v", err)
		consumerCount = 0
	}

	hasConsumerData := consumerCount > 0
	dt.logger.Printf("Consumer data available: %t (%d records)", hasConsumerData, consumerCount)

	dateFilter := strings.Join(targetDates, "','")

	var finalQuery string

	if hasConsumerData {
		finalQuery = fmt.Sprintf(`
			WITH filtered_device_data AS (
				SELECT 
					device_id,
					event_timestamp,
					latitude,
					longitude
				FROM device_data
				WHERE load_date IN ('%s')
			),
			time_filtered_devices AS (
				SELECT * FROM filtered_device_data
				WHERE (toHour(event_timestamp) = 2 AND toMinute(event_timestamp) >= 0)
				   OR (toHour(event_timestamp) = 3)  
				   OR (toHour(event_timestamp) = 4 AND toMinute(event_timestamp) <= 30)
			),
			device_movement_stats AS (
				SELECT 
					device_id,
					avg(latitude) as avg_latitude,
					avg(longitude) as avg_longitude,
					min(event_timestamp) as visited_time,
					count() as ping_count,
					stddevPop(latitude) as lat_std,
					stddevPop(longitude) as lon_std
				FROM time_filtered_devices
				GROUP BY device_id
			),
			idle_devices AS (
				SELECT 
					device_id,
					avg_latitude as latitude,
					avg_longitude as longitude,
					visited_time,
					ping_count,
					lat_std,
					lon_std
				FROM device_movement_stats
				WHERE ping_count >= %d
				  AND lat_std < %f
				  AND lon_std < %f
			),
			consumer_matches AS (
				SELECT 
					i.device_id,
					i.visited_time,
					c.id as consumer_id,
					concat(c.PersonFirstName, ' ', c.PersonLastName) as name,
					c.PrimaryAddress as consumer_address,
					c.Email as email,
					c.CityName as city,
					c.State as state,
					c.ZipCode as zip_code,
					sqrt(
						pow((i.longitude - c.longitude) * 111320 * cos(radians(i.latitude)), 2) + 
						pow((i.latitude - c.latitude) * 110540, 2)
					) as distance_meters,
					ROW_NUMBER() OVER (PARTITION BY i.device_id ORDER BY distance_meters) as rn
				FROM idle_devices i
				INNER JOIN consumers c ON (
					c.latitude BETWEEN i.latitude - 0.0002 AND i.latitude + 0.0002
					AND c.longitude BETWEEN i.longitude - 0.0002 AND i.longitude + 0.0002
				)
				WHERE sqrt(
					pow((i.longitude - c.longitude) * 111320 * cos(radians(i.latitude)), 2) + 
					pow((i.latitude - c.latitude) * 110540, 2)
				) <= %f
			)
			SELECT 
				device_id as DeviceID,
				formatDateTime(visited_time, '%%Y-%%m-%%d') as "Date visited",
				formatDateTime(visited_time, '%%H:%%M:%%S') as "Time visited",
				name as Name,
				consumer_address as Address,
				email as Email,
				consumer_address as POI,
				'' as Campaign,
				city as CityName,
				state as State,
				zip_code as ZipCode
			FROM consumer_matches
			WHERE rn = 1
			ORDER BY visited_time DESC`,
			dateFilter,
			dt.config.Processing.MinPings,
			dt.config.Processing.MovementThreshold,
			dt.config.Processing.MovementThreshold,
			dt.config.Processing.SearchRadius)
	} else {
		finalQuery = fmt.Sprintf(`
			WITH filtered_device_data AS (
				SELECT 
					device_id,
					event_timestamp,
					latitude,
					longitude
				FROM device_data
				WHERE load_date IN ('%s')
			),
			time_filtered_devices AS (
				SELECT * FROM filtered_device_data
				WHERE (toHour(event_timestamp) = 2 AND toMinute(event_timestamp) >= 0)
				   OR (toHour(event_timestamp) = 3)  
				   OR (toHour(event_timestamp) = 4 AND toMinute(event_timestamp) <= 30)
			),
			device_movement_stats AS (
				SELECT 
					device_id,
					avg(latitude) as avg_latitude,
					avg(longitude) as avg_longitude,
					min(event_timestamp) as visited_time,
					count() as ping_count,
					stddevPop(latitude) as lat_std,
					stddevPop(longitude) as lon_std
				FROM time_filtered_devices
				GROUP BY device_id
			),
			idle_devices AS (
				SELECT 
					device_id,
					avg_latitude as latitude,
					avg_longitude as longitude,
					visited_time,
					ping_count,
					lat_std,
					lon_std
				FROM device_movement_stats
				WHERE ping_count >= %d
				  AND lat_std < %f
				  AND lon_std < %f
			)
			SELECT 
				device_id as DeviceID,
				formatDateTime(visited_time, '%%Y-%%m-%%d') as "Date visited",
				formatDateTime(visited_time, '%%H:%%M:%%S') as "Time visited",
				'' as Name,
				'' as Address,
				'' as Email,
				'' as POI,
				'' as Campaign,
				'' as CityName,
				'' as State,
				'' as ZipCode
			FROM idle_devices
			ORDER BY visited_time DESC`,
			dateFilter,
			dt.config.Processing.MinPings,
			dt.config.Processing.MovementThreshold,
			dt.config.Processing.MovementThreshold)
	}

	dt.logger.Println("Executing single comprehensive query...")
	dt.logger.Printf("Query memory settings: max_memory_usage=50GB, external_group_by=20GB, external_sort=20GB")

	rows, err := dt.conn.Query(ctx, finalQuery)
	if err != nil {
		return fmt.Errorf("failed to execute final query: %w", err)
	}
	defer rows.Close()

	// Prepare output file
	outputDir := "./output"
	os.MkdirAll(outputDir, 0755)

	file, err := os.Create(filepath.Join(outputDir, "Device_Tracker_Output.csv"))
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
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
		return fmt.Errorf("failed to write header: %w", err)
	}

	dt.logger.Println("Processing query results...")
	count := 0
	for rows.Next() {
		var deviceID, dateVisited, timeVisited, name, address, email, poi, campaign, city, state, zip string
		if err := rows.Scan(&deviceID, &dateVisited, &timeVisited, &name, &address, &email, &poi, &campaign, &city, &state, &zip); err != nil {
			dt.logger.Printf("Warning: Failed to scan row: %v", err)
			continue
		}

		record := []string{deviceID, dateVisited, timeVisited, name, address, email, poi, campaign, city, state, zip}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
		count++

		// Progress logging and periodic flush
		if count%10000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("csv writer error: %w", err)
			}
			dt.logger.Printf("Processed %d records...", count)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("csv writer error: %w", err)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	processingTime := time.Since(startTime)
	dt.logger.Printf("Single-query analysis completed in %v", processingTime)
	dt.logger.Printf("Exported %d records to CSV", count)

	return nil
}

func (dt *DeviceTracker) Close() error {
	return dt.conn.Close()
}
