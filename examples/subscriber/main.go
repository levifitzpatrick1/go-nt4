package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	nt4 "github.com/levifitzpatrick1/go-nt4"
)

func run(ctx context.Context) error {
	c, err := nt4.NewClient(nt4.ClientOptions{ServerAddress: "127.0.0.1"})
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.Subscribe([]string{"/robot/"}, nt4.SubscriptionOptions{Prefix: true, BufferCapacity: 128, BufferMaxBytes: 1 << 20})
	if err != nil {
		return err
	}
	defer s.Close()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-s.Events():
			if !ok {
				return nil
			}
			fmt.Printf("event %d %s epoch %d timestamp %d\n", ev.Kind, ev.Topic.Name, ev.Epoch, ev.Sample.Timestamp)
		case <-ticker.C:
			if sample, ok := c.Latest("/robot/speed"); ok {
				fmt.Printf("cached speed: %v (epoch %d, stale %v)\n", sample.Value, sample.Epoch, sample.Stale)
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
