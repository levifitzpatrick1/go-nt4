package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/levifitzpatrick1/go-nt4"
)

func main() {
	// Create client options for team 2064
	// Use "127.0.0.1" for simulation or nt4.TeamNumberToAddress(2064) for real robot
	opts := nt4.DefaultClientOptions("127.0.0.1") // Simulation
	// opts := nt4.DefaultClientOptions(nt4.TeamNumberToAddress(2064)) // Real robot: 10.20.64.2

	client := nt4.NewClient(opts)

	// Connect with retry (30 second timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Connecting to NT4 server...")
	if err := client.ConnectWithRetry(ctx); err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect()

	fmt.Println("Connected! Publishing data...")

	// Publish different types of topics
	speedTopic := client.PublishDouble("/robot/speed", 0.0)
	enabledTopic := client.PublishBoolean("/robot/enabled", false)
	positionTopic := client.PublishDoubleArray("/robot/position", []float64{0.0, 0.0, 0.0})

	// Publish values every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-ticker.C:
			counter++

			// Update values
			speed := float64(counter % 100)
			enabled := counter%2 == 0

			client.SetValue(speedTopic, speed)
			client.SetValue(enabledTopic, enabled)
			client.SetValue(positionTopic, []float64{
				float64(counter),
				float64(counter * 2),
				float64(counter * 3),
			})

			fmt.Printf("[%d] Published: speed=%.1f, enabled=%v\n", counter, speed, enabled)

		case <-ctx.Done():
			fmt.Println("Shutting down...")
			return
		}
	}
}
