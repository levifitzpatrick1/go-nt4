package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	nt4 "github.com/levifitzpatrick1/go-nt4"
)

func run(ctx context.Context) error {
	// Connect to local simulation, or use .Team(2064) for a real roboRIO.
	c, err := nt4.NewClientBuilder().
		Server("127.0.0.1").
		Name("publisher-example").
		Build()
	if err != nil {
		return err
	}
	defer c.Close()
	speed, err := c.Publish("/robot/speed", nt4.TypeDouble, nil, nt4.PublisherOptions{})
	if err != nil {
		return err
	}
	defer speed.Close()
	enabled, err := c.Publish("/robot/enabled", nt4.TypeBoolean, nil, nt4.PublisherOptions{})
	if err != nil {
		return err
	}
	defer enabled.Close()
	position, err := c.Publish("/robot/position", nt4.TypeDoubleArray, nil, nt4.PublisherOptions{})
	if err != nil {
		return err
	}
	defer position.Close()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := speed.Set(float64(i)); err != nil {
				return err
			}
			if err := enabled.Set(i%2 == 0); err != nil {
				return err
			}
			if err := position.Set([]float64{float64(i), 0, 0}); err != nil {
				return err
			}
		}
	}
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}
