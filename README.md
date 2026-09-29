# go-nt4

[![CI](https://github.com/levifitzpatrick1/go-nt4/actions/workflows/ci.yaml/badge.svg)](https://github.com/levifitzpatrick1/go-nt4/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/levifitzpatrick1/go-nt4.svg)](https://pkg.go.dev/github.com/levifitzpatrick1/go-nt4)
[![Go Report Card](https://goreportcard.com/badge/github.com/levifitzpatrick1/go-nt4)](https://goreportcard.com/report/github.com/levifitzpatrick1/go-nt4)

Go implementation of the WPILib NT4 protocol.

## Installation

```bash
go get github.com/levifitzpatrick1/go-nt4
```

## Usage

```go
package main

import (
	"fmt"
	"log"
	"time"

	nt4 "github.com/levifitzpatrick1/go-nt4"
)

func main() {
	// Connect to NT4 server at 10.20.64.2
	opts := nt4.DefaultClientOptions(nt4.TeamNumberToAddress(2064))
	opts.OnConnect = func() {
		fmt.Println("NT4 Client Connected")
	}
	opts.OnDisconnect = func() {
		fmt.Println("NT4 Client Disconnected")
	}

	client, err := nt4.NewClient(opts)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Connect(); err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect()

	// Publish a topic and send data
	pub, err := client.PublishDouble("/SmartDashboard/Speed", 42.5)
	if err != nil {
		log.Fatal(err)
	}
	_ = pub.Set(43.0)

	// Subscribe to a topic prefix with a callback
	sub, err := client.SubscribeWithPrefix("/SmartDashboard/", func(topic *nt4.Topic, timestamp int64, value any) {
		fmt.Printf("Callback received: %s = %v\n", topic.Name, value)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Receive data from channel
	go func() {
		for update := range sub.Updates() {
			fmt.Printf("Channel update: %s = %v\n", update.Topic.Name, update.Value)
		}
	}()

	// Retrieve a single value with timeout
	speed, ok := client.GetDouble("/SmartDashboard/Speed", 2*time.Second)
	if ok {
		fmt.Printf("Speed retrieved: %v\n", speed)
	}

	// Read from cache
	if sample, ok := client.Latest("/SmartDashboard/Speed"); ok {
		fmt.Printf("Cached Speed: %v (timestamp: %d)\n", sample.Value, sample.Timestamp)
	}
}
```

## Features

### Client Builder

```go
client, err := nt4.NewClientBuilder().
	Team(2064).
	Name("Dashboard").
	Build()
```

### Typed Publish Methods

```go
client.PublishDouble("/SmartDashboard/Speed", 42.5)
client.PublishString("/SmartDashboard/Mode", "teleop")
client.PublishBoolean("/SmartDashboard/Enabled", true)
client.PublishInt("/SmartDashboard/ShooterRPM", 3500)
client.PublishDoubleArray("/SmartDashboard/Position", []float64{1.0, 2.0, 3.0})
client.PublishStringArray("/SmartDashboard/Cameras", []string{"front", "back"})
client.PublishRaw("/Vision/TargetBytes", []byte{0xDE, 0xAD, 0xBE, 0xEF})
```

### Typed Getters

```go
speed, ok := client.GetDouble("/SmartDashboard/Speed", 2*time.Second)
mode, ok := client.GetString("/SmartDashboard/Mode", 2*time.Second)
enabled, ok := client.GetBoolean("/SmartDashboard/Enabled", 2*time.Second)
rpm, ok := client.GetInt("/SmartDashboard/ShooterRPM", 2*time.Second)
pos, ok := client.GetDoubleArray("/SmartDashboard/Position", 2*time.Second)
```

### Prefix Subscriptions

```go
// Subscribe to all topics starting with /SmartDashboard
sub, err := client.SubscribeWithPrefix("/SmartDashboard/", func(topic *nt4.Topic, timestamp int64, value any) {
	fmt.Printf("%s: %v\n", topic.Name, value)
})
defer sub.Close()

for update := range sub.Updates() {
	fmt.Printf("%s: %v\n", update.Topic.Name, update.Value)
}
```

### Subscriptions & Events

```go
sub, err := client.Subscribe([]string{"/CameraPublisher/"}, nt4.SubscriptionOptions{
	Prefix:         true,
	All:            true,
	Mode:           nt4.DeliveryAll,
	BufferCapacity: 256,
})
defer sub.Close()

for ev := range sub.Events() {
	switch ev.Kind {
	case nt4.Announced:
		fmt.Println("Announced:", ev.Topic.Name, ev.Topic.Type)
	case nt4.ValueReceived:
		fmt.Printf("Value: %s = %v\n", ev.Topic.Name, ev.Sample.Value)
	case nt4.ServerUnannounced:
		fmt.Println("Unannounced:", ev.Topic.Name)
	}
}
```

### Connection with Retry

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := client.ConnectWithRetry(ctx); err != nil {
	log.Fatal("Failed to connect:", err)
}
```

## Data Types

| NT4 Constant | Type String | Go Type | Description |
|---|---|---|---|
| `nt4.TypeBoolean` | `"boolean"` | `bool` | Boolean |
| `nt4.TypeDouble` | `"double"` | `float64` | 64-bit float |
| `nt4.TypeInt` | `"int"` | `int64` | 64-bit integer |
| `nt4.TypeFloat` | `"float"` | `float32` | 32-bit float |
| `nt4.TypeString` | `"string"` | `string` | String |
| `nt4.TypeRaw` | `"raw"` | `[]byte` | Binary bytes |
| `nt4.TypeBooleanArray` | `"boolean[]"` | `[]bool` | Boolean array |
| `nt4.TypeDoubleArray` | `"double[]"` | `[]float64` | Float array |
| `nt4.TypeIntArray` | `"int[]"` | `[]int64` | Integer array |
| `nt4.TypeFloatArray` | `"float[]"` | `[]float32` | Float array |
| `nt4.TypeStringArray` | `"string[]"` | `[]string` | String array |
| `nt4.TypeJSON` | `"json"` | `string` | JSON string |

## Examples

See the [`examples/`](examples/) directory for complete working examples:

- [`publisher`](examples/publisher/): Publishing topics and values
- [`subscriber`](examples/subscriber/): Subscribing to topics
- [`subscriber_callback`](examples/subscriber_callback/): Using callbacks
- [`bidirectional`](examples/bidirectional/): Publishing and subscribing simultaneously
- [`team_robot`](examples/team_robot/): Connecting to an FRC robot

## Documentation

Full API documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/levifitzpatrick1/go-nt4).

## License

MIT
