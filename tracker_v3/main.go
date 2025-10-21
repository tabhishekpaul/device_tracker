package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConfig struct {
	URI        string
	Database   string
	Collection string
}

type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type DeviceEntry struct {
	DeviceID       string    `bson:"deviceid"`
	EventTimestamp time.Time `bson:"eventtimestamp"`
}

type DeviceDocument struct {
	ProcessedDate string        `bson:"processed_date"`
	Devices       []DeviceEntry `bson:"devices"`
}

// GetDeviceIDsByProcessedDates retrieves all unique device IDs for given processed dates
func GetDeviceIDsByProcessedDates(config MongoConfig, processedDates []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.URI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database(config.Database).Collection(config.Collection)

	filter := bson.M{
		"processed_date": bson.M{
			"$in": processedDates,
		},
	}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents: %w", err)
	}
	defer cursor.Close(ctx)

	deviceIDMap := make(map[string]bool)

	for cursor.Next(ctx) {
		var doc DeviceDocument
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Error decoding document: %v", err)
			continue
		}

		for _, device := range doc.Devices {
			deviceIDMap[device.DeviceID] = true
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	deviceIDs := make([]string, 0, len(deviceIDMap))
	for deviceID := range deviceIDMap {
		deviceIDs = append(deviceIDs, deviceID)
	}

	return deviceIDs, nil
}

// getConnection creates a new ClickHouse connection optimized for 40-core, 386GB Azure machine
func getConnection(chConfig ClickHouseConfig) (clickhouse.Conn, error) {
	numCPU := runtime.NumCPU()

	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", chConfig.Host, chConfig.Port)},
		Auth: clickhouse.Auth{
			Database: chConfig.Database,
			Username: chConfig.Username,
			Password: chConfig.Password,
		},
		MaxOpenConns:    numCPU * 5, // 200 connections for 40 cores
		MaxIdleConns:    numCPU * 3, // 120 idle connections
		ConnMaxLifetime: 4 * time.Hour,
		DialTimeout:     30 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":                       36000,
			"send_timeout":                             7200,
			"receive_timeout":                          7200,
			"max_block_size":                           2000000, // Increased for 40 cores
			"max_insert_block_size":                    2000000,
			"min_insert_block_size_rows":               2000000,
			"min_insert_block_size_bytes":              536870912, // 512MB
			"optimize_aggregation_in_order":            1,
			"parallel_view_processing":                 1,
			"compile_expressions":                      1,
			"min_count_to_compile_expression":          3,
			"optimize_read_in_order":                   1,
			"merge_tree_min_rows_for_concurrent_read":  10000,
			"merge_tree_min_bytes_for_concurrent_read": 5242880,
			"max_read_buffer_size":                     20971520, // 20MB
			"join_algorithm":                           "parallel_hash",
			"network_compression_method":               "lz4",
			"max_insert_threads":                       40, // Match CPU cores
		},
	})
}

// ProcessIdleDevicesForDeviceIDs processes idle devices using MAXIMUM parallelization
func ProcessIdleDevicesForDeviceIDs(chConfig ClickHouseConfig, deviceIDs []string) error {
	if len(deviceIDs) == 0 {
		return fmt.Errorf("no device IDs provided")
	}

	conn, err := getConnection(chConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx := context.Background()
	startTime := time.Now()

	log.Printf("Processing %d devices using MAXIMUM CPU parallelization\n", len(deviceIDs))

	// Step 0: Truncate idle_devices table before starting
	log.Println("Truncating idle_devices table...")
	if err := conn.Exec(ctx, `TRUNCATE TABLE device_tracking.idle_devices`); err != nil {
		return fmt.Errorf("failed to truncate idle_devices table: %w", err)
	}
	log.Println("idle_devices table truncated successfully")

	// MAXIMUM PARALLELIZATION SETTINGS for 40-core Azure machine
	numCPU := 48
	runtime.GOMAXPROCS(numCPU) // Ensure Go uses all CPUs

	chunkSize := 5000        // Smaller chunks for 40 cores = more parallelism
	numWorkers := numCPU * 6 // 240 workers for 40 cores (6x multiplier)

	log.Printf("Azure Machine: %d cores, 386GB RAM - Using %d parallel workers with chunk size %d\n",
		numCPU, numWorkers, chunkSize)

	if err := processDevicesInParallelChunks(chConfig, deviceIDs, chunkSize, numWorkers); err != nil {
		return fmt.Errorf("failed to process devices: %w", err)
	}

	// Verify results
	var count uint64
	row := conn.QueryRow(ctx, "SELECT COUNT(*) FROM device_tracking.idle_devices")
	if err := row.Scan(&count); err != nil {
		log.Printf("Warning: Could not verify count: %v", err)
	} else {
		log.Printf("Total idle devices inserted: %d", count)
	}

	log.Printf("Total processing time: %v\n", time.Since(startTime))
	return nil
}

// processDevicesInParallelChunks processes device IDs with MAXIMUM parallelization
func processDevicesInParallelChunks(chConfig ClickHouseConfig, deviceIDs []string, chunkSize, numWorkers int) error {
	totalDevices := len(deviceIDs)
	totalChunks := (totalDevices + chunkSize - 1) / chunkSize

	type chunkJob struct {
		devices []string
		num     int
	}

	// Buffered channels for maximum throughput
	jobs := make(chan chunkJob, numWorkers*2)
	errors := make(chan error, numWorkers)
	var wg sync.WaitGroup
	var processedCount int64
	var errorOccurred atomic.Bool

	// Create MAXIMUM worker pool
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := getConnection(chConfig)
			if err != nil {
				errors <- fmt.Errorf("worker %d: failed to connect: %w", workerID, err)
				errorOccurred.Store(true)
				return
			}
			defer conn.Close()

			for job := range jobs {
				if errorOccurred.Load() {
					return
				}

				if err := processDeviceChunk(conn, job.devices, workerID, job.num, totalChunks); err != nil {
					errors <- fmt.Errorf("worker %d chunk %d: %w", workerID, job.num, err)
					errorOccurred.Store(true)
					return
				}
				atomic.AddInt64(&processedCount, int64(len(job.devices)))
			}
		}(w)
	}

	// Send jobs to workers with maximum speed
	go func() {
		for i := 0; i < totalDevices; i += chunkSize {
			if errorOccurred.Load() {
				break
			}

			end := i + chunkSize
			if end > totalDevices {
				end = totalDevices
			}

			jobs <- chunkJob{
				devices: deviceIDs[i:end],
				num:     (i / chunkSize) + 1,
			}
		}
		close(jobs)
	}()

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	log.Printf("Processed %d devices across %d chunks using %d workers",
		processedCount, totalChunks, numWorkers)
	return nil
}

// processDeviceChunk processes a single chunk with MAXIMUM ClickHouse settings
func processDeviceChunk(conn clickhouse.Conn, deviceIDs []string, workerID, chunkNum, totalChunks int) error {
	ctx := context.Background()
	startTime := time.Now()

	// Create temporary table for this chunk
	tempTableName := fmt.Sprintf("temp_chunk_%d_%d", workerID, time.Now().UnixNano())

	createTempQuery := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			device_id String
		) ENGINE = Memory
	`, tempTableName)

	if err := conn.Exec(ctx, createTempQuery); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Batch insert with MAXIMUM block size
	insertBatch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (device_id)", tempTableName))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, deviceID := range deviceIDs {
		if err := insertBatch.Append(deviceID); err != nil {
			return fmt.Errorf("failed to append: %w", err)
		}
	}

	if err := insertBatch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	// MAXIMUM PERFORMANCE QUERY optimized for 40 cores, 386GB RAM
	processQuery := fmt.Sprintf(`
		INSERT INTO device_tracking.idle_devices (
			device_id,
			home_latitude,
			home_longitude,
			visit_count,
			unique_days,
			first_seen,
			last_seen
		)
		SELECT
			tf.device_id,
			AVG(tf.latitude) AS home_latitude,
			AVG(tf.longitude) AS home_longitude,
			COUNT(*) AS visit_count,
			1 AS unique_days,
			MIN(tf.event_timestamp) AS first_seen,
			MAX(tf.event_timestamp) AS last_seen
		FROM device_tracking.time_filtered AS tf
		INNER JOIN %s AS temp ON tf.device_id = temp.device_id
		GROUP BY tf.device_id
		HAVING 
			dateDiff('second', first_seen, last_seen) >= 3600
			AND 111320 * sqrt(
				pow((max(tf.latitude) - min(tf.latitude)), 2) +
				pow((max(tf.longitude) - min(tf.longitude)) * cos(radians(home_latitude)), 2)
			) <= 25
		SETTINGS 
			max_execution_time = 36000,
			max_memory_usage = 350000000000,
			max_bytes_before_external_group_by = 200000000000,
			max_threads = 40,
			max_block_size = 2000000,
			max_insert_block_size = 2000000,
			max_insert_threads = 40,
			optimize_aggregation_in_order = 1,
			compile_expressions = 1,
			parallel_view_processing = 1,
			join_algorithm = 'parallel_hash',
			join_use_nulls = 0,
			max_rows_in_join = 0,
			partial_merge_join_optimizations = 1
	`, tempTableName)

	if err := conn.Exec(ctx, processQuery); err != nil {
		return fmt.Errorf("failed to process chunk: %w", err)
	}

	log.Printf("Worker %d: Chunk %d/%d completed (%d devices) in %v",
		workerID, chunkNum, totalChunks, len(deviceIDs), time.Since(startTime))

	return nil
}

// MatchDevicesToConsumers matches idle devices to nearby consumers with MAXIMUM performance
func MatchDevicesToConsumers(chConfig ClickHouseConfig) error {
	conn, err := getConnection(chConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer conn.Close()

	ctx := context.Background()
	startTime := time.Now()

	log.Println("Starting device-to-consumer matching with MAXIMUM CPU utilization...")

	// Truncate device_consumer_matches table
	log.Println("Truncating device_consumer_matches table...")
	if err := conn.Exec(ctx, `TRUNCATE TABLE device_tracking.device_consumer_matches`); err != nil {
		return fmt.Errorf("failed to truncate device_consumer_matches table: %w", err)
	}
	log.Println("device_consumer_matches table truncated successfully")

	// MAXIMUM PERFORMANCE MATCHING QUERY for 40 cores, 386GB RAM
	log.Println("Matching devices to consumers within 30 meters...")
	matchQuery := `
		INSERT INTO device_tracking.device_consumer_matches (
			consumer_id,
			PersonFirstName,
			PersonLastName,
			PrimaryAddress,
			TenDigitPhone,
			Email,
			CityName,
			State,
			ZipCode,
			consumer_latitude,
			consumer_longitude,
			device_id,
			device_latitude,
			device_longitude,
			distance_meters
		)
		SELECT
			c.id AS consumer_id,
			c.PersonFirstName,
			c.PersonLastName,
			c.PrimaryAddress,
			c.TenDigitPhone,
			c.Email,
			c.CityName,
			c.State,
			c.ZipCode,
			c.Latitude AS consumer_latitude,
			c.Longitude AS consumer_longitude,
			id.device_id,
			id.home_latitude AS device_latitude,
			id.home_longitude AS device_longitude,
			6371000 * 2 * asin(sqrt(
				pow(sin(radians(c.Latitude - id.home_latitude) / 2), 2) +
				cos(radians(id.home_latitude)) * cos(radians(c.Latitude)) *
				pow(sin(radians(c.Longitude - id.home_longitude) / 2), 2)
			)) AS distance_meters
		FROM device_tracking.idle_devices AS id
		INNER JOIN device_tracking.consumers AS c ON
			c.Latitude BETWEEN id.home_latitude - 0.00027 AND id.home_latitude + 0.00027
			AND c.Longitude BETWEEN id.home_longitude - 0.00027 AND id.home_longitude + 0.00027
			AND 6371000 * 2 * asin(sqrt(
				pow(sin(radians(c.Latitude - id.home_latitude) / 2), 2) +
				cos(radians(id.home_latitude)) * cos(radians(c.Latitude)) *
				pow(sin(radians(c.Longitude - id.home_longitude) / 2), 2)
			)) <= 30
		SETTINGS 
			max_execution_time = 36000,
			max_memory_usage = 350000000000,
			max_bytes_before_external_group_by = 200000000000,
			max_threads = 40,
			max_block_size = 2000000,
			max_insert_block_size = 2000000,
			max_insert_threads = 40,
			join_algorithm = 'parallel_hash',
			optimize_read_in_order = 1,
			compile_expressions = 1,
			parallel_view_processing = 1,
			join_use_nulls = 0,
			max_rows_in_join = 0,
			partial_merge_join_optimizations = 1
	`

	if err := conn.Exec(ctx, matchQuery); err != nil {
		return fmt.Errorf("failed to execute match query: %w", err)
	}

	log.Printf("Matching completed in %v\n", time.Since(startTime))

	// Verify results
	var matchCount uint64
	row := conn.QueryRow(ctx, "SELECT COUNT(*) FROM device_tracking.device_consumer_matches")
	if err := row.Scan(&matchCount); err != nil {
		log.Printf("Warning: Could not verify match count: %v", err)
	} else {
		log.Printf("Total device-consumer matches found: %d", matchCount)
	}

	// Get unique device count with matches
	var uniqueDevices uint64
	deviceRow := conn.QueryRow(ctx, "SELECT COUNT(DISTINCT device_id) FROM device_tracking.device_consumer_matches")
	if err := deviceRow.Scan(&uniqueDevices); err != nil {
		log.Printf("Warning: Could not verify unique device count: %v", err)
	} else {
		log.Printf("Unique devices with consumer matches: %d", uniqueDevices)
	}

	return nil
}

func main() {
	// Set GOMAXPROCS to use all available CPUs
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	log.Printf("Using all %d CPU cores for MAXIMUM performance\n", numCPU)

	mongoConfig := MongoConfig{
		URI:        "mongodb://admin:nyros%4006@172.173.97.164:27017/",
		Database:   "locatrix",
		Collection: "devices_within_campaign",
	}

	chConfig := ClickHouseConfig{
		Host:     "172.173.97.164",
		Port:     9000,
		Database: "device_tracking",
		Username: "default",
		Password: "nyros",
	}

	// Get device IDs from MongoDB for multiple processed dates
	processedDates := []string{"20251019"}

	log.Println("Fetching device IDs from MongoDB...")
	deviceIDs, err := GetDeviceIDsByProcessedDates(mongoConfig, processedDates)
	if err != nil {
		log.Fatalf("Error getting device IDs: %v", err)
	}

	log.Printf("Found %d unique device IDs from MongoDB\n", len(deviceIDs))

	if len(deviceIDs) == 0 {
		log.Println("No device IDs found. Exiting.")
		return
	}

	// Process idle devices using MAXIMUM parallel processing
	log.Println("Processing idle devices with MAXIMUM CPU utilization...")
	if err := ProcessIdleDevicesForDeviceIDs(chConfig, deviceIDs); err != nil {
		log.Fatalf("Error processing idle devices: %v", err)
	}

	log.Println("All idle device processing completed successfully!")

	// Match devices to consumers within 30 meters
	log.Println("\n=== Starting Device-Consumer Matching with MAXIMUM Performance ===")
	if err := MatchDevicesToConsumers(chConfig); err != nil {
		log.Fatalf("Error matching devices to consumers: %v", err)
	}

	log.Println("\n=== All Processing Completed Successfully! ===")
}
