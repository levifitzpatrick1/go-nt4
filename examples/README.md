# Examples

This directory contains standalone, runnable examples demonstrating how to use `go-nt4`.

Compile all examples:
```bash
go build ./examples/...
```

## Available Examples

- **[`publisher`](publisher/)**: Connects to the server, announces topics using typed constants (`nt4.TypeDouble`, `nt4.TypeBoolean`, etc.), and publishes periodic values.
- **[`subscriber`](subscriber/)**: Subscribes to topics, listens for updates on the `sub.Events()` channel, and reads cached snapshots with `client.Latest()`.
- **[`subscriber_callback`](subscriber_callback/)**: Demonstrates processing incoming data with an application callback function inside a message loop.
- **[`bidirectional`](bidirectional/)**: Demonstrates publishing data and subscribing to other topics simultaneously using a single client connection.
- **[`team_robot`](team_robot/)**: Demonstrates connecting directly to an FRC robot using team number addressing (`nt4.NewClientBuilder().Team(2064)`).
