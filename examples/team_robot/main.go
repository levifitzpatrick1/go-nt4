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
	// Connect to Team 2064 roboRIO (resolves to "10.20.64.2:5810").
	c, err := nt4.NewClientBuilder().
		Team(2064).
		Name("dashboard-2064").
		Build()
	if err != nil {
		return err
	}
	defer c.Close()
	sub, err := c.Subscribe([]string{"/FMSInfo/", "/CameraPublisher/"}, nt4.SubscriptionOptions{Prefix: true, BufferCapacity: 256, BufferMaxBytes: 1 << 20})
	if err != nil {
		return err
	}
	defer sub.Close()
	if err := c.Start(ctx); err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := c.WaitConnected(wait); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case state, ok := <-c.StateChanges():
			if !ok {
				return nil
			}
			fmt.Printf("connection state=%d epoch=%d ready=%v dropped=%d\n", state.State, state.Epoch, state.Ready, state.StateChangesDropped)
		case ev, ok := <-sub.Events():
			if !ok {
				return nil
			}
			fmt.Printf("topic %s kind=%d epoch=%d\n", ev.Topic.Name, ev.Kind, ev.Epoch)
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
