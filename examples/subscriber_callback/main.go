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

	client := nt4.NewClient(opts)

	// Connect with retry (30 second timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Connecting to NT4 server...")
	if err := client.ConnectWithRetry(ctx); err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Disconnect()

	fmt.Println("Connected! Subscribing to /robot/speed with callback...")

	// Subscribe to specific topic with callback
	sub := client.Subscribe([]string{"/robot/speed"}, nil)
	defer client.Unsubscribe(sub)

	// Set callback that prints immediately on each update
	sub.SetCallback(func(topic *nt4.Topic, timestamp int64, value any) {
		fmt.Printf("[CALLBACK] %s = %v (timestamp: %d)\n", topic.Name, value, timestamp)
	})

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Listening for updates... Press Ctrl+C to exit")
	<-sigChan

	fmt.Println("\nShutting down...")
}
