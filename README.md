# go-nt4

[![CI](https://github.com/levifitzpatrick1/go-nt4/actions/workflows/ci.yaml/badge.svg)](https://github.com/levifitzpatrick1/go-nt4/actions/workflows/ci.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/levifitzpatrick1/go-nt4.svg)](https://pkg.go.dev/github.com/levifitzpatrick1/go-nt4)
[![Go Report Card](https://goreportcard.com/badge/github.com/levifitzpatrick1/go-nt4)](https://goreportcard.com/report/github.com/levifitzpatrick1/go-nt4)

Go implementation of the WPILib NT4 protocol

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
    "github.com/levifitzpatrick1/go-nt4"
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

    client := nt4.NewClient(opts)

    if err := client.Connect(); err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect()

    // Publish a topic and send data
    examplePub := client.Publish("/SmartDashboard/CoolNumber", nt4.TypeInt, nil)
    client.SetValue(examplePub, int64(123456))

    // Subscribe to a topic
    exampleSub := client.Subscribe([]string{"/SmartDashboard/Example"}, nil)

    // Receive data from subscription with a callback or channel
    exampleSub.SetCallback(func(topic *nt4.Topic, timestamp int64, value any) {
        fmt.Printf("Received data from callback: %v\n", value)
    })

    for update := range exampleSub.Updates() {
        fmt.Printf("Received data from channel: %v\n", update.Value)
    }

    // Temporarily subscribe to a topic to retrieve its data
    oneTimeValue := client.SubscribeAndRetrieve("/SmartDashboard/ConstantValue", 2500*time.Millisecond)

    if oneTimeValue != nil {
        fmt.Printf("Received one time value from server: %v\n", oneTimeValue)
    } else {
        fmt.Println("Client received no value after 2.5 seconds")
    }
}
```

## Additional Features

### Typed Publish Methods

```go
client.PublishDouble("/SmartDashboard/Speed", 42.5)
client.PublishString("/SmartDashboard/Mode", "teleop")
client.PublishBoolean("/SmartDashboard/Enabled", true)
client.PublishDoubleArray("/SmartDashboard/Position", []float64{1.0, 2.0, 3.0})
```

### Typed Getters

```go
speed, ok := client.GetDouble("/SmartDashboard/Speed", 2*time.Second)
mode, ok := client.GetString("/SmartDashboard/Mode", 2*time.Second)
enabled, ok := client.GetBoolean("/SmartDashboard/Enabled", 2*time.Second)
```

### Prefix Subscriptions

```go
// Subscribe to all topics starting with /SmartDashboard
sub := client.Subscribe([]string{"/SmartDashboard"}, &nt4.SubscribeOptions{
    Prefix: true,
    All:    true,
})
```

### Connection with Retry

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := client.ConnectWithRetry(ctx); err != nil {
    log.Fatal("Failed to connect:", err)
}
```

## Examples

See the [`examples/`](examples/) directory for complete working examples.

## Documentation

Full API documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/levifitzpatrick1/go-nt4).

## License

MIT
