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
	c, err := nt4.NewClient(nt4.ClientOptions{})
	if err != nil {
		return err
	}
	defer c.Close()
	p, err := c.Publish("/sensors/temperature", nt4.TypeDouble, nil, nt4.PublisherOptions{})
	if err != nil {
		return err
	}
	defer p.Close()
	status, err := c.Publish("/sensors/status", nt4.TypeString, nil, nt4.PublisherOptions{})
	if err != nil {
		return err
	}
	defer status.Close()
	sub, err := c.Subscribe([]string{"/sensors/"}, nt4.SubscriptionOptions{Prefix: true, BufferCapacity: 128, BufferMaxBytes: 1 << 16})
	if err != nil {
		return err
	}
	defer sub.Close()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.Set(20.0 + float64(i%20)*0.5); err != nil {
				return err
			}
			if err := status.Set("OK"); err != nil {
				return err
			}
		case ev, ok := <-sub.Events():
			if !ok {
				return nil
			}
			if ev.Kind == nt4.ValueReceived {
				fmt.Printf("%s = %v\n", ev.Topic.Name, ev.Sample.Value)
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
