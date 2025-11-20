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
	// Create client options for team 2064
	// Use "127.0.0.1" for simulation or nt4.TeamNumberToAddress(2064) for real robot
	opts := nt4.DefaultClientOptions("127.0.0.1") // Simulation
	// opts := nt4.DefaultClientOptions(nt4.TeamNumberToAddress(2064)) // Real robot: 10.20.64.2

	// Add connection callbacks
	opts.OnConnect = func() {
		fmt.Println("Connected to server")
	}
	opts.OnDisconnect = func() {
		fmt.Println("Disconnected from server")
	}

	client := nt4.NewClient(opts)

	// Connect with retry (30 second timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Connecting to NT4 server...")
	if err := client.ConnectWithRetry(ctx); err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect()

	// Publish our own topics
	fmt.Println("Publishing topics...")
	temperatureTopic := client.PublishDouble("/sensors/temperature", 20.0)
	statusTopic := client.PublishString("/sensors/status", "OK")

	// Subscribe to all topics to see what else is being published
	fmt.Println("Subscribing to all topics...")
	sub := client.Subscribe([]string{""}, &nt4.SubscribeOptions{
		Prefix: true,
		All:    true,
	})
	defer client.Unsubscribe(sub)

	// Handle updates from other sources
	go func() {
		for update := range sub.Updates() {
			// Don't print our own topics to avoid spam
			if update.Topic.Name != "/sensors/temperature" &&
				update.Topic.Name != "/sensors/status" {
				fmt.Printf("[RECEIVED] %s = %v\n", update.Topic.Name, update.Value)
			}
		}
	}()

	// Publish sensor data every 3 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\nBidirectional communication active. Press Ctrl+C to exit")

	temperature := 20.0
	for {
		select {
		case <-ticker.C:
			// Simulate temperature sensor
			temperature += 0.5
			if temperature > 30.0 {
				temperature = 20.0
			}

			client.SetValue(temperatureTopic, temperature)
			client.SetValue(statusTopic, "OK")
			fmt.Printf("[SENT] temperature=%.1f°C\n", temperature)

		case <-sigChan:
			fmt.Println("\nShutting down...")
			return
		}
	}
}
