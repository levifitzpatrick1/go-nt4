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

	fmt.Println("Connected! Subscribing to topics...")

	// Subscribe to all /robot topics with prefix matching
	sub := client.Subscribe([]string{"/robot"}, &nt4.SubscribeOptions{
		Prefix: true,
		All:    true,
	})
	defer client.Unsubscribe(sub)

	// Print updates on a timer to avoid spam
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Collect latest values
	latestValues := make(map[string]any)

	// Process updates in background
	go func() {
		for update := range sub.Updates() {
			latestValues[update.Topic.Name] = update.Value
		}
	}()

	// Print collected values every 2 seconds
	fmt.Println("\nReceiving updates (printing every 2 seconds):")
	for {
		select {
		case <-ticker.C:
			if len(latestValues) == 0 {
				fmt.Println("  No updates received yet...")
				continue
			}

			fmt.Println("\n--- Latest Values ---")
			for topic, value := range latestValues {
				fmt.Printf("  %s = %v\n", topic, value)
			}

		case <-ctx.Done():
			fmt.Println("\nShutting down...")
			return
		}
	}
}
