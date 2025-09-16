package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
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
			FilterOutTime:     "06:00:00",
			MovementThreshold: 0.0001,
			SearchRadius:      100.0,
			MinIdlePings:      3,
			MaxWorkers:        4,
			BatchSize:         10000,
		},
	}

	tracker, err := NewDeviceTracker(config)
	if err != nil {
		log.Fatalf("Failed to create device tracker: %v", err)
	}
	defer tracker.Close()

	startDateStr := "2025-09-15"
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

	ctx := context.Background()
	if err := tracker.RunCampaignConsumerAnalysis(ctx, targetDates); err != nil {
		log.Fatalf("Failed to run campaign consumer analysis: %v", err)
	}

	log.Println("Campaign consumer analysis completed successfully!")
}
