package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
	// Configuration
	config := Config{
		ClickHouse: ClickHouseConfig{
			Host:     "172.173.97.164",
			Port:     9000,
			Database: "device_tracking",
			Username: "default",
			Password: "nyros",
		},
		Processing: ProcessingConfig{
			FilterInTime:      "02:00:00",
			FilterOutTime:     "04:30:00",
			MovementThreshold: 0.0001,
			SearchRadius:      20.0,
			MinPings:          3,
			MaxWorkers:        10,
		},
	}

	startDateStr := "2025-08-23"
	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		panic(err)
	}

	// Number of dates you want (including start date)
	n := 8

	// Collect target dates
	var targetDates []string
	for i := 0; i < n; i++ {
		date := startDate.AddDate(0, 0, -i) // subtract i days
		targetDates = append(targetDates, date.Format("2006-01-02"))
	}

	fmt.Println(targetDates)

	// Create device tracker
	tracker, err := NewDeviceTracker(config)
	if err != nil {
		log.Fatalf("Failed to create device tracker: %v", err)
	}
	defer tracker.Close()

	// Run the complete analysis
	ctx := context.Background()
	if err := tracker.RunCompleteAnalysis(ctx, targetDates); err != nil {
		log.Fatalf("Analysis failed: %v", err)
	}

	log.Println("Analysis completed successfully!")
}
