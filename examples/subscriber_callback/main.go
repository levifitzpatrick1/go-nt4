package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	nt4 "github.com/levifitzpatrick1/go-nt4"
)

// A callback-style consumer is an application loop, not a library dispatcher.
func run(ctx context.Context, onSample func(nt4.Event)) error {
	c, err := nt4.NewClient(nt4.ClientOptions{})
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.Subscribe([]string{"/robot/speed"}, nt4.SubscriptionOptions{BufferCapacity: 64, BufferMaxBytes: 1 << 16})
	if err != nil {
		return err
	}
	defer s.Close()
	if err := c.Start(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-s.Events():
			if !ok {
				return nil
			}
			if ev.Kind == nt4.ValueReceived {
				onSample(ev)
			}
		}
	}
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, func(ev nt4.Event) {
		fmt.Printf("%s: %v @ %d (epoch %d)\n", ev.Topic.Name, ev.Sample.Value, ev.Sample.Timestamp, ev.Epoch)
	}); err != nil {
		log.Fatal(err)
	}
}
