package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type ConsumerParquetLoader struct {
	chConn driver.Conn
	logger *log.Logger
}

func NewConsumerParquetLoader(chConfig ClickHouseConfig) (*ConsumerParquetLoader, error) {
	logger := log.New(os.Stdout, "[ConsumerParquetLoader] ", log.LstdFlags|log.Lmicroseconds)

	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", chConfig.Host, chConfig.Port)},
		Auth: clickhouse.Auth{
			Database: chConfig.Database,
			Username: chConfig.Username,
			Password: chConfig.Password,
		},
		DialTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := chConn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &ConsumerParquetLoader{chConn: chConn, logger: logger}, nil
}

func (cpl *ConsumerParquetLoader) LoadConsumersFromParquet(ctx context.Context, parquetFolder string) error {
	cpl.logger.Printf("Loading consumers from parquet files in %s", parquetFolder)

	files, err := filepath.Glob(filepath.Join(parquetFolder, "*.parquet"))
	if err != nil {
		return fmt.Errorf("failed to list parquet files: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no parquet files found in %s", parquetFolder)
	}

	totalRecords := 0
	batchSize := 1000

	for idx, filePath := range files {
		cpl.logger.Printf("Processing file %d/%d: %s", idx+1, len(files), filepath.Base(filePath))

		f, err := os.Open(filePath)
		if err != nil {
			cpl.logger.Printf("Error opening file: %v", err)
			continue
		}
		defer f.Close()

		reader, err := file.NewParquetReader(f)
		if err != nil {
			return fmt.Errorf("failed to create parquet reader: %w", err)
		}
		defer reader.Close()

		arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.NewGoAllocator())
		if err != nil {
			return fmt.Errorf("failed to create arrow reader: %w", err)
		}

		recordReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
		if err != nil {
			return fmt.Errorf("failed to get record reader: %w", err)
		}
		defer recordReader.Release()

		batch, err := cpl.chConn.PrepareBatch(ctx, `INSERT INTO consumers 
			(id, latitude, longitude, PersonFirstName, PersonLastName, PrimaryAddress, TenDigitPhone, Email, CityName, State, ZipCode)`)
		if err != nil {
			return fmt.Errorf("failed to prepare batch: %w", err)
		}

		for recordReader.Next() {
			rec := recordReader.Record()
			rows := int(rec.NumRows())
			fields := rec.Schema().Fields()

			colIndex := make(map[string]int)
			for i, f := range fields {
				colIndex[f.Name] = i
			}

			for i := 0; i < rows; i++ {
				id := uint64(0)
				lat := float64(0)
				long := float64(0)
				firstName, lastName, address, phone, email, city, state, zip := "", "", "", "", "", "", "", ""

				if idx, ok := colIndex["id"]; ok {
					if col, ok := rec.Column(idx).(*array.Int64); ok {
						id = uint64(col.Value(i))
					}
				}

				if idx, ok := colIndex["latitude"]; ok {
					switch col := rec.Column(idx).(type) {
					case *array.Float64:
						lat = col.Value(i)
					case *array.String:
						val := col.Value(i)
						if f, err := strconv.ParseFloat(val, 64); err == nil {
							lat = f
						}
					}
				}

				if idx, ok := colIndex["longitude"]; ok {
					switch col := rec.Column(idx).(type) {
					case *array.Float64:
						long = col.Value(i)
					case *array.String:
						val := col.Value(i)
						if f, err := strconv.ParseFloat(val, 64); err == nil {
							long = f
						}
					}
				}

				if lat < -90 || lat > 90 || long < -180 || long > 180 {
					continue
				}

				if idx, ok := colIndex["PersonFirstName"]; ok {
					firstName = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["PersonLastName"]; ok {
					lastName = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["PrimaryAddress"]; ok {
					address = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["TenDigitPhone"]; ok {
					phone = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["Email"]; ok {
					email = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["CityName"]; ok {
					city = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["State"]; ok {
					state = rec.Column(idx).(*array.String).Value(i)
				}
				if idx, ok := colIndex["ZipCode"]; ok {
					zip = rec.Column(idx).(*array.String).Value(i)
				}

				if err := batch.Append(id, lat, long, firstName, lastName, address, phone, email, city, state, zip); err != nil {
					cpl.logger.Printf("Error appending: %v", err)
					continue
				}

				totalRecords++
				if totalRecords%batchSize == 0 {
					if err := batch.Send(); err != nil {
						return fmt.Errorf("batch send error: %w", err)
					}
					batch, _ = cpl.chConn.PrepareBatch(ctx, `INSERT INTO consumers 
						(id, latitude, longitude, PersonFirstName, PersonLastName, PrimaryAddress, TenDigitPhone, Email, CityName, State, ZipCode)`)
				}
			}
			rec.Release()
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send final batch for %s: %w", filePath, err)
		}

		runtime.GC()
		cpl.logger.Printf("Processed file %s", filepath.Base(filePath))
	}

	cpl.logger.Printf("✅ Loaded %d records from %d parquet files", totalRecords, len(files))
	return nil
}

func (cpl *ConsumerParquetLoader) Close() {
	if cpl.chConn != nil {
		cpl.chConn.Close()
	}
}

func main() {
	chConfig := ClickHouseConfig{
		Host:     "172.173.97.164",
		Port:     9000,
		Database: "device_tracking",
		Username: "default",
		Password: "nyros",
	}

	loader, err := NewConsumerParquetLoader(chConfig)
	if err != nil {
		log.Fatalf("Failed to create loader: %v", err)
	}
	defer loader.Close()

	ctx := context.Background()
	parquetPath := "/home/device-tracker/data/output/consumer_raw/"
	if err := loader.LoadConsumersFromParquet(ctx, parquetPath); err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}
}
