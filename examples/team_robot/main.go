package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/levifitzpatrick1/go-nt4"
)

func main() {
	// Example for connecting to a real FRC robot (team 2064)
	// This demonstrates using TeamNumberToAddress helper

	teamNumber := 2064
	address := nt4.TeamNumberToAddress(teamNumber)

	fmt.Printf("Team %d robot address: %s\n", teamNumber, address)

	opts := nt4.DefaultClientOptions(address)
	opts.Identity = "Dashboard-2064"

	// Set up callbacks for connection events
	opts.OnConnect = func() {
		fmt.Println("✓ Connected to robot!")
	}
	opts.OnDisconnect = func() {
		fmt.Println("✗ Lost connection to robot")
	}
	opts.OnTopicAnnounce = func(topic *nt4.Topic) {
		fmt.Printf("[NEW TOPIC] %s (%s)\n", topic.Name, topic.Type)
	}

	client := nt4.NewClient(opts)

	// Connect with longer timeout for real robot
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Println("\nAttempting to connect to robot...")
	fmt.Println("Make sure you're on the robot's network!")

	if err := client.ConnectWithRetry(ctx); err != nil {
		log.Fatal("Failed to connect to robot:", err)
	}
	defer client.Disconnect()

	// Subscribe to all topics to see what the robot is publishing
	sub := client.Subscribe([]string{""}, &nt4.SubscribeOptions{
		Prefix: true,
		All:    true,
	})
	defer client.Unsubscribe(sub)

	// Display all topics and their current values
	latestValues := make(map[string]any)

	go func() {
		for update := range sub.Updates() {
			latestValues[update.Topic.Name] = update.Value
		}
	}()

	// Print dashboard every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\n=== Robot Dashboard ===")
	fmt.Println("Press Ctrl+C to exit")

	for {
		select {
		case <-ticker.C:
			fmt.Println("\n--- Robot Status ---")
			if len(latestValues) == 0 {
				fmt.Println("  Waiting for data from robot...")
				continue
			}

			// Print important robot values
			for topic, value := range latestValues {
				fmt.Printf("  %s = %v\n", topic, value)
			}

			// Print server sync info
			offset := client.GetServerTimeOffset()
			rtt := client.GetLastRTT()
			fmt.Printf("\n  [Sync] RTT: %dµs, Offset: %dµs\n", rtt, offset)

		case <-sigChan:
			fmt.Println("\nDisconnecting from robot...")
			return
		}
	}
}
